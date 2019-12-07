import os
import math
import subprocess
import argparse
from typing import List, Dict, Union
from datetime import datetime as dt

import yaml

import aws_utils.aws_management as mgmt

LOG = '../logs'
MANIFESTS = '../cluster_manifests'
KEYS = '../keys'

CRED = '\033[91m'
CEND = '\033[0m'


def _kops_cmd(params: List[str],
              outfile: str = None, outmode='w+', logfile: str = None) -> subprocess.CompletedProcess:
    """
    Executes a kops command with the specified parameters.
    Throws exception when spawned process returns non-zero, also captures stdout and stderr.
    """
    if outfile is not None:
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
    if logfile is not None:
        os.makedirs(os.path.dirname(logfile), exist_ok=True)

    if outfile is not None and logfile is not None:
        with open(outfile, outmode) as out, open(logfile, 'a+') as log:
            result = subprocess.run(['kops'] + params, check=True, stdout=out, stderr=log)
    elif outfile is not None:
        with open(outfile, outmode) as out:
            result = subprocess.run(['kops'] + params, check=True, stdout=out)
    elif logfile is not None:
        with open(logfile, 'a+') as log:
            result = subprocess.run(['kops'] + params, check=True, stdout=log, stderr=log)
    else:
        result = subprocess.run(['kops'] + params, check=True, capture_output=True)
    return result


def _build_config(specs: Dict[str, Union[str, int, float]], logfile: str = None):
    """
    Builds the initial cluster config yaml and writes it to the cluster_manifests folder.
    """

    # see best practices for HA clusters
    # https://github.com/kubernetes/kops/blob/master/docs/operations/high_availability.md#advanced-example
    if len(specs['zones']) < 3:
        master_zones = specs['zones'][-1]
    else:
        master_zones = ','.join(specs['zones'])

    params = [
        'create', 'cluster',
        '--cloud=aws',
        '--master-zones=' + ','.join(specs['zones']),
        '--zones=' + master_zones,
        '--master-count=' + str(specs['masters']),
        '--node-count=' + str(specs['nodes']),
        '--master-size=' + specs['master_size'],
        '--node-size=' + specs['node_size'],
        '--networking=calico',
        '--topology=public',
        '--state=' + specs['state_store'],
        '--dry-run',
        '--output=yaml',
        specs['cluster_name'] + '.k8s.local'
    ]
    manifest_file = os.path.join(MANIFESTS, specs['cluster_name'] + '.yaml')
    ret = _kops_cmd(params, outfile=manifest_file, outmode='w+', logfile=logfile)
    if not specs['log']:
        print(ret.stderr.decode('utf-8'))


def _edit_config(specs: Dict[str, Union[str, int, float]]):
    """
    Reads the config yaml from the cluster_manifests folder and edits specs that cant be set from command line.
    """
    with open(os.path.join(MANIFESTS, specs['cluster_name'] + '.yaml'), 'r') as stream:
        # first file is kind: Cluster, followed by (multiple) kind: InstanceGroup
        config = list(yaml.load_all(stream, Loader=yaml.Loader))
    for group in config[1:]:
        if specs['spot'] == 'all' and 'master' in group['metadata']['name']:
            group['spec']['maxPrice'] = str(specs['spot_master'])
        elif specs['spot'] == 'all' and 'node' in group['metadata']['name']:
            group['spec']['maxPrice'] = str(specs['spot_node'])
        elif specs['spot'] == 'node' and 'node' in group['metadata']['name']:
            group['spec']['maxPrice'] = str(specs['spot_node'])
        else:
            break
    config[0]['spec']['configBase'] = specs['state_store']
    with open(os.path.join(MANIFESTS, specs['cluster_name'] + '.yaml'), 'w') as stream:
        yaml.dump_all(config, stream)


def k8s_cluster(specs: Dict[str, Union[str, int, float]]):
    '''
    Starts a k8s cluster according to the specs. Checks if s3 holds the state of an earlier run
    and launches the old config (ignoring specs) or overrides the old specs (when --override flag is set).
    '''
    specs['state_store'] = 's3://' + specs['cluster_name'].replace('_', '-') + '-state-store'

    timestamp = dt.utcnow().timestamp()
    kops_log = os.path.join(
        LOG, specs['cluster_name'] + '_' + str(timestamp) + '.txt') if specs['log'] else None

    exists = mgmt._s3_kops_config_exist(bucket=specs['state_store'][5:])
    if not exists or (exists and specs['override']):
        #
        specs['zones'] = mgmt._ec2_get_Ncheapest_zones(math.ceil(specs['masters'] / 2),
                                                       specs['region'], instance_type=specs['master_size'])
        mgmt._ec2_create_key_pair(name=specs['cluster_name'], region=specs['region'], path=KEYS)
        specs['key'] = os.path.join(KEYS, specs['cluster_name'] + '.pub')
        # either create new state store or reset existing one
        if not exists:
            mgmt._s3_setup(name=specs['state_store'][5:], region=specs['region'])
        else:
            mgmt._s3_delete_all_keys(bucket=specs['state_store'][5:])
        # build cluster config yaml file
        _build_config(specs, kops_log)
        _edit_config(specs)
        # create new cluster config in state store
        ret = _kops_cmd(['create', '-f',
                         os.path.join(MANIFESTS, specs['cluster_name'] + '.yaml'),
                         '--state=' + specs['state_store']],
                        logfile=kops_log)
        if not specs['log']:
            print(ret.stderr.decode('utf-8'))
        # set ssh public key
        ret = _kops_cmd(['create', 'secret', '--name', specs['cluster_name'] + '.k8s.local',
                         'sshpublickey', 'admin', '-i',
                         os.path.join(KEYS, specs['cluster_name'] + '.pub'),
                         '--state=' + specs['state_store']],
                        logfile=kops_log)
        if not specs['log']:
            print(ret.stderr.decode('utf-8'))

    ret = _kops_cmd(['update', 'cluster', specs['cluster_name'] + '.k8s.local',
                     '--state=' + specs['state_store'],
                     '--yes'], logfile=kops_log)
    if not specs['log']:
        print(ret.stderr.decode('utf-8'))
    print(CRED + specs['cluster_name'] + '.k8s.local is running.' + CEND)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cluster_name', type=str,
                        help='The name of the cluster.')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='The name of the AWS region. Defaults to us-east-1.')
    parser.add_argument('--masters', type=int, default=1,
                        help='The number of masters. Defaults to 1 and should be odd. ')
    parser.add_argument('--master-size', type=str, default='c4.large',
                        help='The instance size of the master(s). Defaults to c4.large.')
    parser.add_argument('--nodes', type=int, default=3,
                        help='The number of nodes. Defaults to 3.')
    parser.add_argument('--node-size', type=str, default='t2.medium',
                        help='The instance size of the node(s). Defaults to t2.medium.')
    parser.add_argument('--spot', type=str, choices=['all', 'node', 'none'], default='all',
                        help='Run cluster entirely on spot (all,default),' \
                             'only nodes on spot (node) or entirely on-demand (none).')
    parser.add_argument('--spot-master', type=float, default=0.25,
                        help='The maximal spot price for master instances. Defaults to $0.25.')
    parser.add_argument('--spot-node', type=float, default=0.25,
                        help='The maximal spot price for node instances. Defaults to $0.25.')
    parser.add_argument('--override', action='store_true',
                        help='Override existing config, is ignored when config does not exist.')
    parser.add_argument('--log', action='store_true',
                        help='Write kops output to logfile.')
    args = parser.parse_args()
    cluster_specs = vars(args)

    k8s_cluster(cluster_specs)
