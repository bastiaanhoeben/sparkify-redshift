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
    iam = boto3.resource('aim', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.resource('redshift', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)

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
    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("Get the IAM role ARN")
    role_arn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
    print(role_arn)

    return role_arn


def create_redshift_cluster(redshift, role_arn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD):
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
            NumberOfNodes=int(DWH_NUM_NODES),

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

    pd.set_option('display.max_colwidth', -1)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in cluster_properties.items() if k in keys_to_show]
    print(pd.DataFrame(data=x, columns=["Key", "Value"]))


# Print cluster endpoint and role ARN
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

# Open VPC TCP port to access cluster endpoint
 try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

# Delete Redshift cluster
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

# Describe Redshift cluster to see its status
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# Detach role policy and delete role
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


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

    HOST=config.get('DB', 'HOST')
    DB_NAME=config.get('DB', 'DB_NAME')
    DB_USER=config.get('DB', 'DB_USER')
    DB_PASSWORD=config.get('DB', 'DB_PASSWORD')
    DB_PORT=config.get('DB', 'DB_PORT')

    # Configure Redshift cluster
    iam, ec2, s3, redshift = create_clients(KEY, SECRET, REGION)
    role_arn = create_iam_role(iam, IAM_ROLE_NAME)
    create_redshift_cluster(redshift, role_arn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD)

    # Print cluster status
    

if __name__ == "__main__":
    main()