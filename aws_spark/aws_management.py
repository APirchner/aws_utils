from datetime import datetime as dt
from typing import Dict, Tuple, List

import boto3


def _iam_setup(user_name: str, group_name: str, policies: Dict[str, str]) -> Tuple[str, str, str]:
    iam_client = boto3.client('iam')

    # set up group with specified policies
    response = iam_client.list_groups()
    group_exists = group_name in [group['GroupName'] for group in response['Groups']]

    if group_exists:
        # reuse if group has the right policies
        response = iam_client.list_group_policies(GroupName=group_name)
        policies_miss = list(set(response['PolicyNames']) - set(policies.keys()))
    else:
        iam_client.create_group(GroupName=group_name)
        policies_miss = list(policies.values())
    for policy in policies_miss:
        iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy)

    # set up user and add to group
    response = iam_client.list_users()
    users = {user['UserName']: user['UserId'] for user in response['Users']}
    if user_name in users:
        user_id = users[user_name]
        iam_client.add_user_to_group(GroupName=group_name, UserName=user_name)
    else:
        response = iam_client.create_user(UserName=user_name)
        user_id = response['User']['UserId']
        iam_client.add_user_to_group(GroupName=group_name, UserName=user_name)

    # check if key quota would be exceeded
    response = iam_client.list_access_keys(UserName=user_name)
    active_keys = len([key for key in response['AccessKeyMetadata'] if key['Status'] == 'Active'])
    if active_keys > 1:
        iam_client.delete_access_key(UserName=user_name,
                                     AccessKeyId=response['AccessKeyMetadata'][-1]['AccessKeyId'])
    response = iam_client.create_access_key(UserName=user_name)
    access_key_id = response['AccessKey']['AccessKeyId']
    secret_key = response['AccessKey']['SecretAccessKey']
    return user_id, access_key_id, secret_key


def _get_Ncheapest_zones(n: int, region: str, instance_type: str) -> List[Tuple[str, float]]:
    ec2_client = boto3.client('ec2', region_name=region)
    response = ec2_client.describe_spot_price_history(InstanceTypes=[instance_type], StartTime=dt.now(),
                                                      ProductDescriptions=['Linux/UNIX'])
    zone_prices = [(zone['AvailabilityZone'], float(zone['SpotPrice'])) for zone in response['SpotPriceHistory']]
    cheapest_zones = sorted(zone_prices, key=lambda x: x[1])[0:min(len(zone_prices, n))]
    return cheapest_zones


def _create_key_pair(name: str, region: str):
    ec2_client = boto3.client('ec2', region_name=region)
    # check if key exists and delete if yes
    keypairs = ec2_client.describe_key_pairs()['KeyPairs']
    for key_pair in keypairs:
        if key_pair['KeyName'] == 'spark-key':
            ec2_client.delete_key_pair(KeyName=key_pair['KeyName'])

    # create new key
    response = ec2_client.create_key_pair(KeyName=name)
    key = {'name': response['KeyName'], 'private_key': response['KeyMaterial']}

    with open("key.pem", 'w') as f:
        f.write(key['private_key'])
    return key


def _s3_setup(name: str, region: str) -> None:
    if region != 'us-east-1':
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket=name, CreateBucketConfiguration={'LocationConstraint': region}
        )
    else:
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=name)
    s3_client.put_bucket_versioning(Bucket=name, VersioningConfiguration={'Status': 'Enabled'})
