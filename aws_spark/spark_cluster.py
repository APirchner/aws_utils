import os
import argparse


import boto3

import aws_spark.aws_management as mgmt





if __name__ == '__main__':

    mgmt._get_cheapest_zone('us-east-1', 'c5.large')

    '''
    
    '''