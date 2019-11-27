import os
import subprocess as sp
from typing import Mapping, List

import yaml

import aws_spark.aws_management as mgmt

CLUSTER_NAME = 'NAME'
STATE_STORE = 'KOPS_STATE_STORE'


def _kops_cmd(cmd: str, params: List[str], environment: Mapping) -> sp.CompletedProcess:
    """ executes a kops command with the specified parameters in the specified environment.
        Throws exception when spawned process returns non-zero, also captures stdout and stderr
    """
    result = sp.run([cmd] + params, check=True, capture_output=True, env=environment)
    return result


def kubernetes_cluster_config():
    pass


def kubernetes_cluster(specs):
    environment = os.environ.copy()
    id, key_id, secret = mgmt._iam_setup('aws_spark', 'aws_spark',
                                         {'AmazonEC2FullAccess': 'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
                                          'AmazonS3FullAccess': 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                                          'AmazonRoute53FullAccess': 'arn:aws:iam::aws:policy/AmazonRoute53FullAccess',
                                          'IAMFullAccess': 'arn:aws:iam::aws:policy/IAMFullAccess',
                                          'AmazonVPCFullAccess': 'arn:aws:iam::aws:policy/AmazonVPCFullAccess'})
    environment['AWS_ACCESS_KEY_ID'] = key_id
    environment['AWS_SECRET_ACCESS_KEY'] = secret