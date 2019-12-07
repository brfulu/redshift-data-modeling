import boto3
import time
import configparser
from botocore.exceptions import ClientError


def create_redshift_cluster(config):
    """Create an Amazon Redshift cluster

    The function returns without waiting for the cluster to be fully created.

    :param config: configparser object; Contains necessary configurations
    :return: dictionary containing cluster information, otherwise None.
    """

    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.create_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_ID'),
            ClusterType='single-node',
            NodeType='dc2.large',
            PubliclyAccessible=True,
            DBName=config.get('CLUSTER', 'DB_NAME'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            Port=config.getint('CLUSTER', 'DB_PORT')
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

    return cluster_info


def main():
    """Test create_redshift_cluster()"""

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    cluster_info = create_redshift_cluster(config)
    if cluster_info is not None:
        print(f'Creating cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')
        print(f'Database name: {cluster_info["DBName"]}')

        print('Waiting for cluster to be created...')
        cluster_info = wait_for_cluster_creation(cluster_info['ClusterIdentifier'])
        print(f'Cluster created.')
        print(f"Endpoint={cluster_info['Endpoint']['Address']}")


if __name__ == '__main__':
    main()
