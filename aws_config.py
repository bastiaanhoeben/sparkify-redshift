import boto3
from configparser import ConfigParser
import json
import pandas as pd


def create_clients(KEY, SECRET, REGION):
    """
    Creates clients for IAM, EC2, S3 and Redshift.
        
        Parameters:
            KEY, SECRET: AWS admin credentials
            REGION: AWS region
        
        Returns: 
            iam: AIM client
            ec2: EC2 client
            s3: S3 client
            redshift: Redshift client
    """

    # Create clients
    iam = boto3.client('iam', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    print("IAM, EC2, S3 and Redshift clients created")

    return iam, ec2, s3, redshift


def create_iam_role(iam, IAM_ROLE_NAME):
    """
    Creates a new IAM role for accessing S3 bucket.
    
        Parameters:
            iam: IAM client
            IAM_ROLE_NAME: IAM role name
        
        Returns:
            role_arn: Role ARN (Amazon Resource Name)
    
    """
    
    # Creating a new IAM Role
    try:
        print("Creating a new IAM role")
        iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
    
    # Attach a role policy
    print("Attaching role policy")
    attach_response = iam.attach_role_policy(RoleName=IAM_ROLE_NAME,PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    print(f"Status of role policy attachment: {attach_response}")

    # Get IAM role ARN
    print("Get the IAM role ARN")
    role_arn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
    print(f"Role_ARN: {role_arn}")

    return role_arn


def create_redshift_cluster(redshift, role_arn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD):
    """
    Creates a Redshift cluster.
        
        Parameters:
            redshift: Redshift instance
            role_arn: IAM Role ARN
            DWH_CLUSTER_TYPE:
            DWH_NODE_TYPE:
            DWH_NUM_NODES: 
            DWH_CLUSTER_IDENTIFIER:
            DB_NAME:
            DB_USER:
            DB_PASSWORD:
    """

    print("Creating Redshift cluster")
    try:
        redshift.create_cluster(        
            #DHW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[role_arn]  
        )
    except Exception as e:
        print(e)


def describe_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Prints a description of the cluster to see its status.
    
        Parameters:
            redshift: Redshift client
            DWH_CLUSTER_IDENTIFIER: Redshift cluster identifier
    """

    cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    pd.set_option('display.max_colwidth', None)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", "VpcId"]
    x = [(k, v) for k,v in cluster_properties.items() if k in keys_to_show]
    status_table = pd.DataFrame(data=x, columns=["Key", "Value"])
    print(status_table)


def get_cluster_endpoint(REGION, KEY, SECRET, DWH_CLUSTER_IDENTIFIER):
    """
    Get Redshift cluster endpoint and print role ARN.
    
        Parameters:
            redshift: Redshift instance
            DWH_CLUSTER_IDENTIFIER: Redshift cluster identifier

        Returns:
            dwh_endpoint: Redshift cluster endpoint    
    """

    redshift = boto3.client('redshift', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    dwh_endpoint = cluster_properties['Endpoint']['Address']
    dwh_role_arn = cluster_properties['IamRoles'][0]['IamRoleArn']

    print(f"Redshift cluster endpoint: {dwh_endpoint}")
    print(f"Redshift Role ARN: {dwh_role_arn}")

    return dwh_endpoint


def open_vpc_ports(ec2, redshift, DWH_CLUSTER_IDENTIFIER, DB_PORT):
    """
    Open VPC TCP port to access cluster endpoint.
    
        Parameters:
            ec2: EC2 instance
            redshift: Redshift instance
            DWH_CLUSTER_IDENTIFIER: Redshift cluster identifier
            DB_PORT: Redshift standard port
    """

    cluster_properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    try:
        vpc = ec2.Vpc(id=cluster_properties['VpcId'])
        default_sg = list(vpc.security_groups.all())[0]
        print(f"Default security group: {default_sg}")
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)

    print(f"Port {DB_PORT} opened")


def delete_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Delete Redshift cluster.

        Parameters:
            redshift: Redshift instance
            DWH_CLUSTER_IDENTIFIER: Redshift cluster identifier
    """

    # Delete cluster
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print("Deleting Redshift cluster")


def delete_iam_role(iam, IAM_ROLE_NAME):
    """
    Detach role policy and delete role.
        
        Parameters:
            iam: iam client
            IAM_ROLE_NAME: IAM role name
    """
    
    iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    iam.delete_role(RoleName=IAM_ROLE_NAME)
    print("IAM Role deleted")


def main():

    # Load relevant parameters from config file
    config = ConfigParser()
    config.read('dwh.cfg')

    KEY=config.get('AWS', 'KEY')
    SECRET=config.get('AWS', 'SECRET')
    REGION=config.get('AWS', 'REGION')

    IAM_ROLE_NAME=config.get('IAM_ROLE', 'IAM_ROLE_NAME')

    DWH_CLUSTER_TYPE=config.get('DWH', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES=config.get('DWH', 'DWH_NUM_NODES')
    DWH_NODE_TYPE=config.get('DWH', 'DWH_NODE_TYPE')
    DWH_CLUSTER_IDENTIFIER=config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')

    DB_NAME=config.get('DB', 'DB_NAME')
    DB_USER=config.get('DB', 'DB_USER')
    DB_PASSWORD=config.get('DB', 'DB_PASSWORD')
    DB_PORT=config.get('DB', 'DB_PORT')

    # Configure Redshift cluster
    iam, ec2, s3, redshift = create_clients(KEY, SECRET, REGION)
    # delete_iam_role(iam, IAM_ROLE_NAME)  # Delete any old iam role
    role_arn = create_iam_role(iam, IAM_ROLE_NAME)
    create_redshift_cluster(redshift, role_arn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD)

    # Print cluster status
    while True:
        user_input = input("Print cluster status? (y/n): ")
        if user_input == "y":
            describe_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
        else:
            break
    
    # Get cluster endpoint
    dwh_endpoint = get_cluster_endpoint(REGION, KEY, SECRET, DWH_CLUSTER_IDENTIFIER)

    # Open redshift port for all inbound traffic
    open_vpc_ports(ec2, redshift, DWH_CLUSTER_IDENTIFIER, DB_PORT)

    # Delete redshift cluster
    while True:
        user_input = input('Delete cluster? (Please type "y" to delete): ')
        if user_input == "y":
            delete_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
            break
    
    # Print cluster status
    while True:
        user_input = input("Print cluster status? (y/n): ")
        if user_input == "y":
            try:
                describe_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
            except:
                print("Cluster deleted")
                break
        else:
            break

    # Delete IAM role for Reshift cluster
    delete_iam_role(iam, IAM_ROLE_NAME)


if __name__ == "__main__":
    main()