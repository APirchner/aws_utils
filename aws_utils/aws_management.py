from typing import Dict

import boto3

def _iam_setup(user_name: str, group_name: str, policies: Dict[str, str]):
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
    access_key = response['AccessKey']['AccessKeyId']
    secret_key = response['AccessKey']['SecretAccessKey']
    return user_id, access_key, secret_key

def _s3_setup(credentials:Dict[str, str], name:str, region:str):
    pass