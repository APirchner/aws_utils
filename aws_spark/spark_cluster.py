import os
import argparse

import boto3

import aws_spark.aws_management as mgmt


CLUSTER_NAME = 'NAME'
STATE_STORE = 'KOPS_STATE_STORE'

KEYID_ENV = 'AWS_ACCESS_KEY_ID'
KEYSEC_ENV = 'AWS_SECRET_ACCESS_KEY'

if __name__ == '__main__':

    mgmt._get_cheapest_zone('us-east-1', 'c5.large')

    '''
    id, key_id, secret = mgmt._iam_setup('aws_spark', 'aws_spark',
                              {'AmazonEC2FullAccess': 'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
                               'AmazonS3FullAccess': 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                               'AmazonRoute53FullAccess': 'arn:aws:iam::aws:policy/AmazonRoute53FullAccess',
                               'IAMFullAccess': 'arn:aws:iam::aws:policy/IAMFullAccess',
                               'AmazonVPCFullAccess': 'arn:aws:iam::aws:policy/AmazonVPCFullAccess'})
    '''