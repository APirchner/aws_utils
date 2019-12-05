import os
import subprocess
import argparse
from typing import Mapping, List, Dict, Union

import yaml

import aws_utils.aws_management as mgmt

MANIFESTS = '../cluster_manifests'
KEYS = '../keys'


def _kops_cmd(params: List[str], environment: Mapping, outfile: str = None) -> subprocess.CompletedProcess:
    """
    Executes a kops command with the specified parameters in the specified environment.
    Throws exception when spawned process returns non-zero, also captures stdout and stderr.
    """
    if outfile is not None:
        with open(outfile, 'w') as file:
            result = subprocess.run(['kops'] + params, check=True, stdout=file, env=environment)
    else:
        result = subprocess.run(['kops'] + params, check=True, capture_output=True, env=environment)
    return result


def _build_config(specs: Dict[str, Union[str, int, float]], environment: Mapping):
    """
    Builds the initial cluster config yaml and writes it to the cluster_manifests folder.
    """

    params = [
        'create', 'cluster',
        '--cloud=aws',
        '--master-zones=' + ','.join(specs['zones']),
        '--zones=' + ','.join(specs['zones']),
        '--master-count=' + str(specs['masters']),
        '--node-count=' + str(specs['nodes']),
        '--master-size=' + specs['master_size'],
        '--node-size=' + specs['node_size'],
        '--networking=calico',
        '--topology=private',
        '--dry-run',
        '-o=yaml',
        environment['NAME']
    ]
    manifest_file = os.path.join(MANIFESTS, specs['cluster_name'] + '.yaml')
    os.makedirs(os.path.dirname(manifest_file), exist_ok=True)
    _kops_cmd(params, environment, outfile=manifest_file)


def _edit_config(specs: Dict[str, Union[str, int, float]]):
    """
    Reads the config yaml from the cluster_manifests folder and edits specs that cant be set from command line.
    """
    with open(os.path.join(MANIFESTS, specs['cluster_name'] + '.yaml'), 'r') as stream:
        # first file is kind: Cluster, followed by (multiple) kind: InstanceGroup
        config = list(yaml.load_all(stream, Loader=yaml.Loader))
    return config


def k8s_cluster(specs: Dict[str, Union[str, int, float]]):
    # set up environment for kops calls
    environment = os.environ.copy()
    environment['NAME'] = specs['cluster_name'] + '.k8s.local'
    environment['KOPS_STATE_STORE'] = 's3://' + specs['cluster_name'].replace('_', '-') + '-state-store'

    # create ssh key pair and set state store bucket if it doesnt exist
    if not mgmt._s3_kops_config_exist(bucket=environment['KOPS_STATE_STORE'][5:]):
        mgmt._s3_setup(name=environment['KOPS_STATE_STORE'][5:], region=specs['region'])
    mgmt._ec2_create_key_pair(name=specs['cluster_name'], region=specs['region'], path=KEYS)
    specs['zones'] = mgmt._ec2_get_Ncheapest_zones(2, specs['region'], instance_type=specs['master_size'])
    _build_config(specs, environment)
    # _edit_config(specs)


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
    parser.add_argument('--spot', type=str, choices=['all', 'node', 'none'],
                        help='Run cluster entirely on spot (all),' \
                             'only nodes on spot (node) or entirely on-demand (none).')
    parser.add_argument('--spot-master', type=float, default=0.25,
                        help='The maximal spot price for master instances. Defaults to $0.25.')
    parser.add_argument('--spot-node', type=float, default=0.25,
                        help='The maximal spot price for node instances. Defaults to $0.25.')
    args = parser.parse_args()
    cluster_specs = vars(args)

    k8s_cluster(cluster_specs)
