import os
import argparse

import boto3

from aws_spark.kubernetes_cluster import kubernetes_cluster

if __name__ == '__main__':
    kubernetes_cluster({'cluster_name': 'andreascluster'})