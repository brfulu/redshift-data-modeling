import boto3
import time
import json
import configparser
from botocore.exceptions import ClientError


def create_redshift_cluster(config, iam_role_arn):
    """Create an Amazon Redshift cluster

    The function returns without waiting for the cluster to be fully created.

    :param config: configparser object; Contains necessary configurations
    :return: dictionary containing cluster information, otherwise None.
    """

    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.create_cluster(
            ClusterIdentifier='redshift-udacity',
            ClusterType='single-node',
            NodeType='dc2.large',
            PubliclyAccessible=True,
            DBName=config.get('CLUSTER', 'DB_NAME'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            Port=config.getint('CLUSTER', 'DB_PORT'),
            IamRoles=[iam_role_arn]
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

    redshift_client = boto3.client('redshift')

    while True:
        response = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            break
        time.sleep(60)

    print(cluster_info)
    return cluster_info


def create_iam_role(config):
    iam_client = boto3.client('iam')
    role = iam_client.create_role(
        RoleName=config.get('IAM_ROLE', 'ROLE_NAME'),
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
        RoleName=config.get('IAM_ROLE', 'ROLE_NAME'),
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    return role


def get_iam_role(config):
    iam_client = boto3.client('iam')
    try:
        response = iam_client.get_role(RoleName=config.get('IAM_ROLE', 'ROLE_NAME'))
    except Exception as e:
        return None
    else:
        return response


def main():
    """Initiate and wait for redshift cluster deletion"""

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    iam_role = get_iam_role(config)
    if iam_role is None:
        iam_role = create_iam_role(config)

    cluster_info = create_redshift_cluster(config, iam_role['Role']['Arn'])

    if cluster_info is not None:
        print(f'Creating cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')
        print(f'Database name: {cluster_info["DBName"]}')

        print('Waiting for cluster to be created...')
        cluster_info = wait_for_cluster_creation(cluster_info['ClusterIdentifier'])
        print(f'Cluster created.')
        print(f"Endpoint={cluster_info['Endpoint']['Address']}")
        print(f"Role_ARN={iam_role['Role']['Arn']}")

if __name__ == '__main__':
    main()
