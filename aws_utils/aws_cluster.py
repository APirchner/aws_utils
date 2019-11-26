import os

import aws_utils.aws_management as mgmt


CLUSTER_NAME = 'NAME'
STATE_STORE = 'KOPS_STATE_STORE'

KEYID_ENV = 'AWS_ACCESS_KEY_ID'
KEYSEC_ENV = 'AWS_SECRET_ACCESS_KEY'

if __name__ == '__main__':
    id, key, secret = mgmt._iam_setup('test', 'test',
                              {'AmazonEC2FullAccess': 'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
                               'AmazonS3FullAccess': 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                               'AmazonRoute53FullAccess': 'arn:aws:iam::aws:policy/AmazonRoute53FullAccess',
                               'IAMFullAccess': 'arn:aws:iam::aws:policy/IAMFullAccess',
                               'AmazonVPCFullAccess': 'arn:aws:iam::aws:policy/AmazonVPCFullAccess'})
    os.environ[KEYID_ENV] = key
    os.environ[KEYSEC_ENV] = secret