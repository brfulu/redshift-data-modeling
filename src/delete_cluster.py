import boto3
import time
import configparser
from botocore.exceptions import ClientError


def delete_redshift_cluster(config):
    """Create an Amazon Redshift cluster

    The function returns without waiting for the cluster to be fully created.

    :param config: configparser object; Contains necessary configurations
    :return: dictionary containing cluster information, otherwise None.
    """

    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_ID'),
            SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        print(f'ERROR: {e}')
        return None
    else:
        return response['Cluster']


def delete_iam_role(config):
    """Delete IAM role for redshift"""

    iam_client = boto3.client('iam')
    try:
        response = iam_client.delete_role(RoleName=config.get('IAM_ROLE', 'ROLE_NAME'))
    except Exception as e:
        print(e)


def main():
    """Initiate redshift cluster deletion"""

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    cluster_info = delete_redshift_cluster(config)
    if cluster_info is not None:
        print(f'Deleting cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')
        delete_iam_role(config)


if __name__ == '__main__':
    main()
