import aws_utils.aws_management as mgmt


if __name__ == '__main__':
    id, key, secret = mgmt._iam_setup('test', 'test',
                              {'AmazonEC2FullAccess': 'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
                               'AmazonS3FullAccess': 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                               'AmazonRoute53FullAccess': 'arn:aws:iam::aws:policy/AmazonRoute53FullAccess',
                               'IAMFullAccess': 'arn:aws:iam::aws:policy/IAMFullAccess',
                               'AmazonVPCFullAccess': 'arn:aws:iam::aws:policy/AmazonVPCFullAccess'})
    print(id)
    print(key)
    print(secret)
