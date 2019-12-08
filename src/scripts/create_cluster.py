import boto3
import time
import json
import configparser
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift', region_name='us-west-2')
iam_client = boto3.client('iam')
ec2_client = boto3.client('ec2', region_name='us-west-2')


def create_redshift_cluster(config, iam_role_arn, cluster_sg_id):
    """Create an Amazon Redshift cluster

    The function returns without waiting for the cluster to be fully created.

    :param config: configparser object; Contains necessary configurations
    :return: dictionary containing cluster information, otherwise None.
    """
    try:
        response = redshift_client.create_cluster(
            ClusterIdentifier='redshift-udacity',
            ClusterType='multi-node',
            NumberOfNodes=4,
            NodeType='dc2.large',
            PubliclyAccessible=True,
            DBName=config.get('CLUSTER', 'DB_NAME'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            Port=config.getint('CLUSTER', 'DB_PORT'),
            IamRoles=[iam_role_arn],
            VpcSecurityGroupIds=[cluster_sg_id]
        )
    except ClientError as e:
        print(f'ERROR: {e}')
        return None
    else:
        return response['Cluster']


def wait_for_cluster_creation(cluster_id):
    """Create an Amazon Redshift cluster

    The function returns without waiting for the cluster to be fully created.

    :param cluster_id: string; Cluster identifier
    :return: dictionary containing cluster information.
    """
    while True:
        response = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            break
        time.sleep(60)

    print(cluster_info)
    return cluster_info


def create_cluster_security_group():
    response = ec2_client.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

    try:
        response = ec2_client.create_security_group(GroupName='myredshiftsg', Description='Redshift security group',
                                                    VpcId=vpc_id)
        security_group_id = response['GroupId']
        print('Security Group Created %s in vpc %s.' % (security_group_id, vpc_id))

        data = ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {'IpProtocol': 'tcp',
                 'FromPort': 80,
                 'ToPort': 80,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp',
                 'FromPort': 5439,
                 'ToPort': 5439,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ])
        return security_group_id
    except ClientError as e:
        print(e)


def create_iam_role(config):
    role = iam_client.create_role(
        RoleName=config.get('SECURITY', 'ROLE_NAME'),
        Description='Allows Redshift to call AWS services on your behalf',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}
            }]
        })
    )

    iam_client.attach_role_policy(
        RoleName=config.get('SECURITY', 'ROLE_NAME'),
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    return role


def main():
    """Initiate and wait for redshift cluster deletion"""

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    cluster_sg_id = create_cluster_security_group()
    iam_role = create_iam_role(config)
    cluster_info = create_redshift_cluster(config, iam_role['Role']['Arn'], cluster_sg_id)

    if cluster_info is not None:
        print(f'Creating cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')
        print(f'Database name: {cluster_info["DBName"]}')

        print('Waiting for cluster to be created...')
        cluster_info = wait_for_cluster_creation(cluster_info['ClusterIdentifier'])
        print(f'Cluster created.')
        print(f"Endpoint={cluster_info['Endpoint']['Address']}")
        print(f"Role_ARN={iam_role['Role']['Arn']}")
        print(f"Security_Group={cluster_sg_id}")


if __name__ == '__main__':
    main()
