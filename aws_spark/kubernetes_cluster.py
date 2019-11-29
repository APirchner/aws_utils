import os
import subprocess as sp
from typing import Mapping, List, Dict, Union

import yaml

import aws_spark.aws_management as mgmt

CLUSTER_NAME = 'NAME'
STATE_STORE = 'KOPS_STATE_STORE'


def _kops_cmd(params: List[str], environment: Mapping, outfile: str = None) -> sp.CompletedProcess:
    """ executes a kops command with the specified parameters in the specified environment.
        Throws exception when spawned process returns non-zero, also captures stdout and stderr
    """
    if outfile is not None:
        with open(outfile, 'w') as file:
            result = sp.run(['kops'] + params, check=True, stdout=file, env=environment)
    else:
        result = sp.run(['kops'] + params, check=True, capture_output=True, env=environment)
    return result


def kubernetes_cluster_config():
    pass


def kubernetes_cluster(specs: Dict[str, Union[str, int, float]]):
    '''
    id, key_id, secret = mgmt._iam_setup(
        user_name='kops', group_name='kops',
        policies={'AmazonEC2FullAccess': 'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
                  'AmazonS3FullAccess': 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                  'AmazonRoute53FullAccess': 'arn:aws:iam::aws:policy/AmazonRoute53FullAccess',
                  'IAMFullAccess': 'arn:aws:iam::aws:policy/IAMFullAccess',
                  'AmazonVPCFullAccess': 'arn:aws:iam::aws:policy/AmazonVPCFullAccess'}
    )
    '''
    cluster_name = specs['cluster_name'] + '.k8s.local'
    bucket_name = specs['cluster_name'].replace('_', '-') + '-state-store'

    environment = os.environ.copy()
    # environment['AWS_ACCESS_KEY_ID'] = key_id
    # environment['AWS_SECRET_ACCESS_KEY'] = secret
    environment['NAME'] = cluster_name
    environment['KOPS_STATE_STORE'] = 's3://' + bucket_name

    mgmt._ec2_create_key_pair(name=cluster_name, region='us-east-1')

    if not mgmt._s3_kops_config_exist(bucket=bucket_name):
        mgmt._s3_setup(name=bucket_name, region='us-east-1')
    try:
        manifest_file = '../cluster_manifests/s' + cluster_name + '.yaml'
        os.makedirs(os.path.dirname(manifest_file), exist_ok=True)
        result = _kops_cmd(['create', 'cluster', cluster_name, '--cloud=aws', '--zones=us-east-1a',
                            '--dry-run', '-o=yaml'], environment,
                           outfile=manifest_file)
    except sp.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr)
    print(result.args)
    # print(mgmt._s3_get_kops_config(bucket=bucket_name, key='/'.join([cluster_name, 'config'])))
