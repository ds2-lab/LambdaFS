import argparse 
import boto3
import botocore 
import json
import logging 
import os 
import paramiko 
import requests 
import shutil
import socket
import subprocess 
import time 
import urllib3
import yaml 

from datetime import datetime
from paramiko.client import SSHClient, AutoAddPolicy
from paramiko.rsakey import RSAKey
from requests import get
from time import sleep
from tqdm import tqdm

Component_LambdaFSPrimaryClientVM = 'PrimaryLambdaFSClientVM'
Component_HopsFSPrimaryClientVM = 'PrimaryHopsFSClientVM'
Component_ZooKeeperVM = "ZooKeeperVM"
Component_NDB_ManagerVM = "NDB_MGM_VM"
Component_NDB_DataNodeVM = "NDB_DataNodeVM"

os.system("color")

"""
This script creates all of the infrastrucutre necessary to run λFS and Vanilla HopsFS, 
and to replicate the experiments conducted in the paper,
"λ FS: A Scalable and Elastic Distributed File System Metadata Service using Serverless Functions"
"""

MYSQL_NDB_MANAGER_AMI = "ami-0ef872c16032b2aeb" # "ami-0a0e055a66e58df2c"
MYSQL_NDB_DATANODE1_AMI = "ami-02b05337b4142f447" # "ami-075e47140b5fd017a"
MYSQL_NDB_DATANODE2_AMI = "ami-0a3d55fdd1fe9a6b8" # "ami-0fdbf79b2ec52386e"
HOPSFS_CLIENT_AMI = "ami-00b50df44bd99ab5f"
HOPSFS_NAMENODE_AMI = "ami-00b50df44bd99ab5f"
LAMBDA_FS_CLIENT_AMI = "ami-0ab2d4e7b34dd78af"
LAMBDA_FS_ZOOKEEPER_AMI = "ami-0700bf4465e5fd16d"

# Starts ZooKeeper.
START_ZK_COMMAND = "sudo /opt/zookeeper/bin/zkServer.sh start"
STOP_MYSQLD_COMMAND = "/usr/local/mysql/bin/mysqladmin -u root shutdown"
START_MYSQLD_COMMAND = "/home/ubuntu/mysql.server start &"

# Set up logging.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

# If True, then print messages will not contain color. Note that colored prints are only supported when running on Linux. 
# This is updated by the command-line arguments. It does not need to be changed manually.
NO_COLOR = False 

# Used to add colors to log messages.
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[33m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def log_error(msg):
    if not NO_COLOR:
        msg = bcolors.FAIL + msg + bcolors.ENDC
    logger.error(msg)

def log_warning(msg):
    if not NO_COLOR:
        msg = bcolors.WARNING + msg + bcolors.ENDC
    logger.warning(msg)

def log_success(msg):
    if not NO_COLOR:
        msg = bcolors.OKGREEN + msg + bcolors.ENDC
    logger.info(msg)

def log_important(msg):
    if not NO_COLOR:
        msg = bcolors.OKCYAN + msg + bcolors.ENDC
    logger.info(msg)

print_success = log_success
print_warning = log_warning
print_error = log_error
print_important = log_important

def create_vpc(
    aws_region:str = "us-east-1",
    vpc_name:str = "LambdaFS_VPC", 
    vpc_cidr_block:str = "10.0.0.0/16", 
    security_group_name:str = "lambda-fs-security-group", 
    user_ip:str = None, 
    ec2_resource = None, 
    ec2_client = None) -> str:
    """
    Create the Virtual Private Cloud that will house all of the infrastructure required by λFS and HopsFS.
    
    Keyword Arguments:
    ------------------
        aws_profile_name (str):
            The AWS credentials profile to use when creating the resources. 
            If None, then this script will ultimately use the default AWS credentials profile.

        NO_COLOR (bool):
            If True, then print messages will not contain color. Note that colored prints are
            only supported when running on Linux.
            
    Returns:
    --------
        str: vpc id
    
        old:
        dict: A dictionary containing various properties of the newly-created VPC. 
        {
            "vpc_id" (str): The ID of the VPC,
            "subnetIds" (list of str): The IDs of the subnets,
            "securityGroupIds" (list of str): The security group IDs (there should only be one),
        }
    """
    if user_ip == None:
        log_error("User IP address cannot be 'None' when creating the AWS VPC.")
        exit(1)
        
    try:
        socket.inet_aton(user_ip)
    except OSError:
        log_error("Invalid user IP address specified when creating AWS VPC: \"%s\"" % user_ip)
        exit(1) 
    
    log_important("Creating VPC \"%s\" now." % vpc_name)
    
    # Create the VPC.
    create_vpc_response = ec2_client.create_vpc(
        CidrBlock = vpc_cidr_block, 
        TagSpecifications = [{
            'ResourceType': 'vpc',
            'Tags': [{
                'Key': 'Name',
                'Value': vpc_name
            }]
        }])
    vpc = ec2_resource.Vpc(create_vpc_response["Vpc"]["VpcId"])
    vpc.wait_until_available()
    
    log_success("Successfully created a VPC. VPC ID: " + vpc.id)
    logger.info("Next, creating two public subnets.")
    
    # Create the first public subnet.
    public_subnet1 = vpc.create_subnet(
        CidrBlock = "10.0.0.0/20",
        AvailabilityZone = aws_region + "a",
        TagSpecifications = [{
            'ResourceType': 'subnet',
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': "serverless-mds-subnet-public1"
                },
                {
                    'Key': 'PrivacyType',
                    'Value': 'public'
                }
            ]}])
    ec2_client.modify_subnet_attribute(SubnetId = public_subnet1.id, MapPublicIpOnLaunch = {'Value': True})
    log_success("Successfully created the first public subnet. Subnet ID: " + public_subnet1.id)

    # Create the second public subnet.
    public_subnet2 = vpc.create_subnet(
        CidrBlock = "10.0.16.0/20",
        AvailabilityZone = aws_region + "b",
        TagSpecifications = [{
            'ResourceType': 'subnet',
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': "serverless-mds-subnet-public2"
                },
                {
                    'Key': 'PrivacyType',
                    'Value': 'public'
                }                
            ]}])
    ec2_client.modify_subnet_attribute(SubnetId = public_subnet2.id, MapPublicIpOnLaunch = {'Value': True})
    log_success("Successfully created the second public subnet. Subnet ID: " + public_subnet2.id)
    # public_subnets = [public_subnet1, public_subnet2]
    
    logger.info("Next, creating two private subnets.")
    
    # Create the first private subnet.
    private_subnet1 = vpc.create_subnet(
        CidrBlock = "10.0.128.0/20",
        AvailabilityZone = aws_region + "a",
        TagSpecifications = [{
        'ResourceType': 'subnet',
        'Tags': [
            {
                'Key': 'Name',
                'Value': "serverless-mds-subnet-private1"
            },
            {
                'Key': 'PrivacyType',
                'Value': 'private'
            }            
        ]
    }])
    log_success("Successfully created the first private subnet. Subnet ID: " + private_subnet1.id)
    
    # Create the second private subnet.
    private_subnet2 = vpc.create_subnet(
        CidrBlock = "10.0.144.0/20",
        AvailabilityZone = aws_region + "b",
        TagSpecifications = [{
        'ResourceType': 'subnet',
        'Tags': [
            {
                'Key': 'Name',
                'Value': "serverless-mds-subnet-private2"
            },
            {
                'Key': 'PrivacyType',
                'Value': 'private'
            }            
        ]
    }])    
    log_success("Successfully created the second private subnet. Subnet ID: " + private_subnet2.id)
    # private_subnets = [private_subnet1, private_subnet2]

    logger.info("Next, creating an internet gateway.")
    # Create and attach an internet gateway.
    create_internet_gateway_response = ec2_client.create_internet_gateway(
        TagSpecifications = [{
            'ResourceType': 'internet-gateway',
            'Tags': [{
                'Key': 'Name',
                'Value': "Lambda-FS-InternetGateway"
            }]
        }])
    internet_gateway_id = create_internet_gateway_response["InternetGateway"]["InternetGatewayId"]
    vpc.attach_internet_gateway(InternetGatewayId = internet_gateway_id)
    
    log_success("Successfully created an Internet Gateway and attached it to the VPC. Internet Gateway ID: " + internet_gateway_id)
    logger.info("Next, allocating elastic IP address and creating NAT gateway.")
    
    elastic_ip_response = ec2_client.allocate_address(
        Domain = 'vpc',
        TagSpecifications = [{
            'ResourceType': 'elastic-ip',
            'Tags': [{
                'Key': 'Name',
                'Value': "lambda-fs-nat-gateway-ip"
            }]
        }])
    nat_gateway = ec2_client.create_nat_gateway(
        SubnetId = public_subnet1.id, 
        AllocationId = elastic_ip_response['AllocationId'], 
        TagSpecifications = [{
            'ResourceType': 'natgateway',
            'Tags': [{
                'Key': 'Name',
                'Value': "LambdaFS-NatGateway"
            }]
        }])
    nat_gateway_id = nat_gateway["NatGateway"]["NatGatewayId"]

    log_success("Successfully allocated elastic IP address and created NAT gateway. NAT Gateway ID: " + nat_gateway_id)
    logger.info("Next, creating route tables and associated public route table with public subnet.")
    logger.info("But first, sleeping for ~45 seconds so that the NAT gateway can be created.")

    for _ in tqdm(range(181)):
        sleep(0.25)
    
    # The VPC creates a route table, so we have one to begin with. We use this as the public route table.
    initial_route_table = list(vpc.route_tables.all())[0] 
    initial_route_table.create_route(
        DestinationCidrBlock = '0.0.0.0/0',
        GatewayId = internet_gateway_id
    )
    initial_route_table.associate_with_subnet(SubnetId = public_subnet1.id)
    initial_route_table.associate_with_subnet(SubnetId = public_subnet2.id)

    # Now create the private route table.
    private_route_table = vpc.create_route_table(
        TagSpecifications = [{
            'ResourceType': 'route-table',
            'Tags': [{
                'Key': 'Name',
                'Value': "LambdaFS-PrivateRouteTable"
            }]
        }])
    private_route_table.create_route(
        DestinationCidrBlock = '0.0.0.0/0',
        GatewayId = nat_gateway_id
    )

    log_success("Successfully created the route tables and associated public route table with public subnet.")
    logger.info("Next, associating private route table with the private subnets.")
    
    # Associate the private route table with each private subnet.
    private_route_table.associate_with_subnet(SubnetId = private_subnet1.id)
    private_route_table.associate_with_subnet(SubnetId = private_subnet2.id)
    
    log_success("Successfully associated the private route table with the private subnets.")
    logger.info("Next, creating and configuring the security group. Security group name: \"%s\"" % security_group_name)
    
    security_group = ec2_resource.create_security_group(
        Description='LambdaFS security group', GroupName = security_group_name, VpcId = vpc.id,
        TagSpecifications = [{
            "ResourceType": "security-group",
            "Tags": [
                {"Key": "Name", "Value": security_group_name}
            ]
        }])
    
    # TODO: In the actual security group I used, there are two other authorization rules, each of which corresponds to something related to EKS. 
    # If EKS requires its own security group, then we'll need to update these rules once we've created the EKS cluster. 
    security_group.authorize_ingress(IpPermissions = [
        { # All traffic that originates from within the security group itself.
            "FromPort": 0,
            "ToPort": 65535,
            "IpProtocol": "-1",
            "UserIdGroupPairs": [{
                "GroupId": security_group.id,
                "VpcId": vpc.id
            }]
        },
        { # SSH traffic from your machine's IP address. 
            "FromPort": 22,
            "ToPort": 22,
            "IpProtocol": "tcp",
            "IpRanges": [{
                "CidrIp": user_ip + "/32", 
                "Description": "SSH from my PC"
            }]
        }
    ])
    
    log_success("Successfully created and configured security group \"%s\"." % security_group_name)
    print()
    print()
    log_success("=======================")
    log_success("λFS VPC setup complete.")
    log_success("=======================")
    
    return vpc.id
    # return {
    #     "vpc_id": vpc.id,
    #     "securityGroupIds": [],
    #     "subnetIds": []        
    # }
    
def create_hops_fs_client_vm(
    ec2_resource = None,
    instance_type:str = "r5.4xlarge",
    ssh_keypair_name:str = None,
    subnet_id:str = None,
    security_group_ids:list = [],
)->str:
    """
    Create the primary HopsFS client VM. Once created, this script should be executed from the λFS client VM to create the remaining AWS infrastructure.
    
    Return:
    -------
        str: the ID of the newly-created HopsFS client VM.
    """
    if ec2_resource == None:
        log_error("EC2 client cannot be null when creating the HopsFS client VM.")
        exit(1)
    
    if ssh_keypair_name == None:
        log_error("SSH keypair name cannot be null when creating the HopsFS client VM.")
        exit(1)
    
    hops_fs_client_vm = ec2_resource.create_instances(
        MinCount = 1,
        MaxCount = 1,
        ImageId = HOPSFS_CLIENT_AMI,
        InstanceType = instance_type,
        KeyName = ssh_keypair_name,
        NetworkInterfaces = [{
            "AssociatePublicIpAddress": True,
            "DeviceIndex": 0,
            "SubnetId": subnet_id,
                "Groups": security_group_ids
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': 
                [{
                    'Key': 'Name',
                    'Value': "hops-fs-client-driver"
                },
                {
                    'Key': 'Project',
                    'Value': 'LambdaFS'
                },
                {
                    'Key': 'Component',
                    'Value': Component_HopsFSPrimaryClientVM
                }]
        }]  
    )
    
    return hops_fs_client_vm[0].id   

def create_lambda_fs_client_vm(
    ec2_resource = None,
    instance_type:str = "r5.4xlarge",
    ssh_keypair_name:str = None,
    subnet_id:str = None,
    security_group_ids:list = [],
)->str:
    """
    Create the primary λFS client VM. Once created, this script should be executed from the λFS client VM to create the remaining AWS infrastructure.
    
    Return:
    -------
        str: the ID of the newly-created λFS client VM.
    """
    if ec2_resource == None:
        log_error("EC2 client cannot be null when creating the λFS client VM.")
        exit(1)
    
    if ssh_keypair_name == None:
        log_error("SSH keypair name cannot be null when creating the λFS client VM.")
        exit(1)
    
    lambda_fs_client_vm = ec2_resource.create_instances(
        MinCount = 1,
        MaxCount = 1,
        ImageId = LAMBDA_FS_CLIENT_AMI,
        InstanceType = instance_type,
        KeyName = ssh_keypair_name,
        NetworkInterfaces = [{
            "AssociatePublicIpAddress": True,
            "DeviceIndex": 0,
            "SubnetId": subnet_id,
                "Groups": security_group_ids
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': 
                [{
                    'Key': 'Name',
                    'Value': "lambda-fs-client-driver"
                },
                {
                    'Key': 'Project',
                    'Value': 'LambdaFS'
                },
                {
                    'Key': 'Component',
                    'Value': Component_LambdaFSPrimaryClientVM
                }]
        }]  
    )
    
    return lambda_fs_client_vm[0].id  

def create_lambda_fs_zookeeper_vms(
    ec2_resource = None,
    ssh_keypair_name:str = None,
    num_vms:int = 3,
    subnet_id:str = None,
    instance_type:str = "r5.4xlarge",
    security_group_ids = [],
):
    """
    Create the λFS ZooKeeper nodes.
    
    Return a list of str of the IDs of the newly-created λFS ZooKeeper nodes.
    """
    if ec2_resource == None:
        log_error("EC2 resource cannot be null when creating the λFS ZooKeeper nodes.")
        exit(1)
    
    if ssh_keypair_name == None:
        log_error("SSH keypair name cannot be null when creating the λFS ZooKeeper nodes.")
        exit(1)
        
    
    logger.info("Creating %d λFS ZooKeeper node(s) of type %s." % (num_vms, instance_type))
    
    zookeeper_node_ids = []
    for i in range(0, num_vms):
        instance_name = "lambdafs-zookeeper-%d" % i
        zoo_keeper_node = ec2_resource.create_instances(
            MinCount = 1,
            MaxCount = 1,
            ImageId = LAMBDA_FS_ZOOKEEPER_AMI,
            InstanceType = instance_type,
            KeyName = ssh_keypair_name,
            NetworkInterfaces = [{
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "SubnetId": subnet_id,
                    "Groups": security_group_ids
            }],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': 
                    [{
                        'Key': 'Name',
                        'Value': instance_name
                    },
                    {
                        'Key': 'Project',
                        'Value': 'LambdaFS'
                    },
                    {
                        'Key': 'Component',
                        'Value': Component_ZooKeeperVM
                    }]
            }]  
        )
        zookeeper_node_ids.append(zoo_keeper_node[0].id)
    
    return zookeeper_node_ids

def create_eks_iam_role(iam, iam_role_name:str = "lambda-fs-eks-cluster-role") -> str:
    """
    Create the IAM Role to be used by the AWS EKS Cluster.
    
    Returns:
    --------
        str: The ARN of the newly-created IAM role.
    """
    trust_relationships = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "Service": ["eks.amazonaws.com"]
            },
            "Action": ["sts:AssumeRole"]
        }],
    }
    
    try:
        role_response = iam.create_role(
            RoleName = iam_role_name, 
            Path = "/",
            Description = "Allows access to other AWS service resources that are required to operate clusters managed by EKS. Used by the Lambda-FS EKS cluster.", 
            AssumeRolePolicyDocument = json.dumps(trust_relationships)) 
    except iam.exceptions.EntityAlreadyExistsException:
        print_warning("Exception encountered when creating IAM role for the AWS Lambda functions: `iam.exceptions.EntityAlreadyExistsException`", no_header = False)
        print_warning("Attempting to fetch ARN of existing role with name \"%s\" now..." % iam_role_name, no_header = True)
        
        try:
            role_response = iam.get_role(RoleName = iam_role_name)
        except iam.exceptions.NoSuchEntityException as ex:
            # This really shouldn't happen, as we tried to create the role and were told that the role exists.
            # So, we'll just terminate the script here. The user needs to figure out what's going on at this point. 
            print_error("Exception encountered while attempting to fetch existing IAM role with name \"%s\": `iam.exceptions.NoSuchEntityException`" % iam_role_name, no_header = False)
            print_error("Please verify that the AWS role exists and re-execute the script. Terminating now.", no_header = True)
            exit(1) 
        
    role_arn = role_response['Role']['Arn']
    print_success("Successfully created IAM role. ARN of newly-created role: \"%s\". Next, attaching required IAM role polices." % role_arn)
    
    iam.attach_role_policy(
        PolicyArn = 'arn:aws:iam::aws:policy/AmazonEKSClusterPolicy',
        RoleName = iam_role_name)
    
    return role_arn

def create_eks_openwhisk_cluster(
    aws_profile_name:str = None, 
    aws_region:str = "us-east-1", 
    vpc_name:str = "LambdaFS_VPC", 
    eks_iam_role_name = "lambda-fs-eks-cluster-role", 
    vpc_id:str = None, 
    eks_cluster_name:str = "lambda-fs-eks-cluster",
    create_eks_iam_role = True,
    ec2_client = None
):
    """
    Create the AWS EKS cluster and deploy OpenWhisk on that cluster.
    """
    if vpc_id == None:
        log_error("VPC ID cannot be null when creating the AWS EKS cluster.")
        exit(1)
        
    if aws_profile_name is not None:
        logger.info("Attempting to create AWS Session using explicitly-specified credentials profile \"%s\" now..." % aws_profile_name)
        try:
            session = boto3.Session(profile_name = aws_profile_name)
            log_success("Successfully created boto3 Session using AWS profile \"%s\"" % aws_profile_name)
        except Exception as ex: 
            log_error("Exception encountered while trying to use AWS credentials profile \"%s\"." % aws_profile_name, no_header = False)
            raise ex 
        
        iam = session.client('iam')
        eks = session.client('eks')
    else:
        iam = boto3.client('iam')
        eks = boto3.client('eks')
    
    logger.info("Creating EKS cluster.")
    
    logger.info("Creating IAM role.")
    
    if create_eks_iam_role:
        role_arn = create_eks_iam_role(iam, iam_role_name = eks_iam_role_name)
    else:
        try:
            response = iam.get_role(RoleName = eks_iam_role_name)
        except iam.exceptions.NoSuchEntityException:
            print()
            log_error("Could not find existing IAM role with name \"%s\"." % eks_iam_role_name)
            log_error("Please verify that the IAM role you specified exists and doesn't contain any typos in the name.")
            exit(1)
        
        role_arn = response['Role']['Arn']
    
    # Get the security group ID(s).
    resp = ec2_client.describe_security_groups(
        Filters = [{
            'Name': 'vpc-id',
            'Values': [vpc_id]   
        }]
    )
    security_group_ids = []
    for security_group in resp['SecurityGroups']:
        security_group_id = security_group['GroupId']
        security_group_ids.append(security_group_id)
    
    # Get the subnet ID(s).
    resp = ec2_client.describe_subnets(
        Filters = [{
            'Name': 'vpc-id',
            'Values': [vpc_id]   
        }]
    )
    subnet_ids = []
    for subnet in resp['Subnets']:
        subnet_id = subnet['SubnetId']
        subnet_ids.append(subnet_id)
    
    # Create AWS EKS cluster.
    response = eks.create_cluster(  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/eks/client/create_cluster.html
        name = eks_cluster_name,    # The unique name to give to your cluster.
        version = "1.24",           # Desired kubernetes version.
        roleArn = role_arn,         # The Amazon Resource Name (ARN) of the IAM role that provides permissions for the Kubernetes control plane to make calls to Amazon Web Services API operations on your behalf.
        resourcesVpcConfig = {      # The VPC configuration that’s used by the cluster control plane.
            "subnetIds": subnet_ids,
            "securityGroupIds": security_group_ids,
            "endpointPublicAccess": True,
            "endpointPrivateAccess": False,
            "publicAccessCidrs": ["0.0.0.0/0"]
        },
        kubernetesNetworkConfig = { # The Kubernetes network configuration for the cluster.
            # "serviceIpv4Cidr": "", # If you don’t specify a block, Kubernetes assigns addresses from either the 10.100.0.0/16 or 172.20.0.0/16 CIDR blocks. Let's just let it do that.
            #"ipFamily": "ipv4"       # Specify which IP family is used to assign Kubernetes pod and service IP addresses. 
        }
    )
    
    cluster_response = response['cluster']
    
    if cluster_response['status'] == 'FAILED':
        log_error("Creation of AWS EKS Cluster has apparently failed.")
        log_error("Full response:")
        log_error(cluster_response)
        exit(1)
    
    log_important("AWS EKS Cluster creation API call succeeded.")
    log_important("According to the AWS documentation, it can typically take 10 - 15 minutes for the cluster to be fully created and become operational.")
    log_important("We will begin creating some of the other components while we wait for the EKS Cluster to finish being created.")

def create_ec2_auto_scaling_group(
    auto_scaling_group_name:str = "",
    launch_template_name:str = "",
    min_size:int = 0,
    max_size:int = 8,
    desired_capacity:int = 0,
    availability_zones:list = [],
    autoscaling_client = None
):
    """
    Create an EC2 auto-scaling group.
    
    Returns a 2-tuple where the first element is the newly-created launch template's name and the second is the template's ID.
    """
    if autoscaling_client == None:
        log_error("Autoscaling client cannot be done when creating an auto-scaling group.")
        exit(1)
        
    logger.info("Creating auto-scaling group \"%s\" with launch template \"%s\"." % (auto_scaling_group_name, launch_template_name))
        
    response = autoscaling_client.create_auto_scaling_group(
        AutoScalingGroupName = auto_scaling_group_name,
        LaunchTemplate = {
            'LaunchTemplateName': launch_template_name,
            'Version': '$Default',
        },
        MinSize = min_size,
        MaxSize = max_size,
        DesiredCapacity = desired_capacity,
        AvailabilityZones = availability_zones,
    )
    
    logger.info("Response from creating auto-scaling group \"%s\": %s" % (auto_scaling_group_name, str(response)))
    
    # asg_name = response
    # asg_id = response
    
    # return asg_name, asg_id

def create_launch_template(
    launch_template_name:str = "",
    launch_template_description:str = "",
    ec2_client = None,
    ami_id:str = "", 
    instance_type:str = "",
    security_group_ids:list = [],
):
    """
    Create an EC2 Launch Template for use with an EC2 Auto-Scaling Group. 
    
    Returns a 2-tuple where the first element is the newly-created launch template's name and the second is the template's ID.
    """
    if ec2_client == None:
        log_error("EC2 client cannot be null when creating a launch template.")
        exit(1)
    
    response = ec2_client.create_launch_template(
        LaunchTemplateName = launch_template_name,
        VersionDescription = launch_template_description,
        LaunchTemplateData = {
            "ImageId": ami_id,
            "InstanceType": instance_type,
            "SecurityGroupIds": security_group_ids,
            "NetworkInterfaces": [{
                'AssociatePublicIpAddress': True,
                'DeviceIndex': 0,
            }]
        }
    )
    
    logger.info("Response from creating launch template \"%s\": %s" % (launch_template_name, str(response)))
    
    template_name = response['LaunchTemplate']['LaunchTemplateName']
    template_id = response['LaunchTemplate']['LaunchTemplateId']
    
    return template_name, template_id 

def create_launch_templates_and_instance_groups(
    ec2_client = None,
    autoscaling_client = None,
    lfs_client_ags_it:str = "r5.4xlarge",
    hopsfs_client_ags_it:str = "r5.4xlarge",
    hopsfs_namenode_ags_it:str = "r5.4xlarge",
    skip_launch_templates:bool = False,
    skip_autoscaling_groups:bool = False,
    security_group_ids:list = [],
    data = {}
):
    """
    Create the launch templates and auto-scaling groups for λFS clients, HopsFS clients, and HopsFS NameNodes.
    """

    if not skip_launch_templates:
        logger.info("Creating the EC2 launch templates now.")
        
        # λFS clients.
        name, id = create_launch_template(ec2_client = ec2_client, launch_template_name = "lambda_fs_clients", launch_template_description = "LambdaFS_Clients_Ver1", ami_id = LAMBDA_FS_CLIENT_AMI, instance_type = lfs_client_ags_it, security_group_ids = security_group_ids)
        
        data["lfs-client-launch-template-name"] = name
        data["lfs-client-launch-template-id"] = id 
        
        # HopsFS clients.
        name, id = create_launch_template(ec2_client = ec2_client, launch_template_name = "hopsfs_clients", launch_template_description = "HopsFS_Clients_Ver1", ami_id = HOPSFS_CLIENT_AMI, instance_type = hopsfs_client_ags_it, security_group_ids = security_group_ids)
        
        data["hospfs-client-launch-template-name"] = name
        data["hospfs-client-launch-template-id"] = id 
        
        # HopsFS NameNodes.
        name, id = create_launch_template(ec2_client = ec2_client, launch_template_name = "hopsfs_namenodes", launch_template_description = "HopsFS_NameNodes_Ver1", ami_id = HOPSFS_NAMENODE_AMI, instance_type = hopsfs_namenode_ags_it, security_group_ids = security_group_ids)
        
        data["hopsfs-nn-launch-template-name"] = name
        data["hopsfs-nn-launch-template-id"] = id 
        
        logger.info("Created the EC2 launch templates.")
    else:
        logger.info("Skipping the creation of the EC2 launch templates.")
    
    if not skip_autoscaling_groups:
        logger.info("Creating the EC2 auto-scaling groups now.")
        
        # λFS clients.
        create_ec2_auto_scaling_group(auto_scaling_group_name = "lambda_fs_clients_ags", autoscaling_client = autoscaling_client, launch_template_name = "lambda_fs_clients")
        # HopsFS clients.
        create_ec2_auto_scaling_group(auto_scaling_group_name = "hopsfs_clients_ags",autoscaling_client = autoscaling_client, launch_template_name = "hopsfs_clients")
        # HopsFS NameNodes.
        create_ec2_auto_scaling_group(auto_scaling_group_name = "hopsfs_namenodes_ags",autoscaling_client = autoscaling_client, launch_template_name = "hopsfs_namenodes")
        
        logger.info("Created the EC2 auto-scaling groups.")
    else:
        logger.info("Skipping the creation of the EC2 auto-scaling groups.") 

def register_openwhisk_namenodes():
    """
    Create and register serverless NameNode functions with the EKS OpenWhisk cluster. 
    """
    pass 

def validate_keypair_exists(ssh_keypair_name = None, ec2_client = None)->bool:
    """
    Return true if there exists an SSH keypair with the given name registered with AWS.
    
    WARNING: Terminates/aborts if the ec2_client or ssh_keypair_name parameter is null!
    """
    if ssh_keypair_name == None:
        print()
        log_error("No SSH keypair specified (value is null).")
        exit(1)
    
    if ec2_client == None:
        log_error("EC2 client is null.")
        exit(1)
    
    try:
        response = ec2_client.describe_key_pairs(KeyNames=[ssh_keypair_name])
    except botocore.exceptions.ClientError as error:
        log_error(error)
        return False 
    
    if len(response['KeyPairs']) > 1:
        log_error("Somehow found multiple KeyPairs for key-pair name \"%s\"" % ssh_keypair_name) 
        for keypair in response['KeyPairs']:
            log_error("Found: \"%s\"" % keypair['KeyName'])
    
    if response['KeyPairs'][0]['KeyName'] == ssh_keypair_name:
        return True 
    
    return False 

def update_zookeeper_config(
    ec2_client = None,
    instance_ids:list = [],
    ssh_key_path:str = None,
    zookeeper_jvm_heap_size:int = 4000,
    data:dict = {}
):
    """
    Update the configuration information on the ZooKeeper VMs.
    
    Returns:
    --------
        list of str: The public IP addresses of the newly-created ZooKeeper nodes.
    """
    
    config = """\
tickTime=1000
dataDir=/data/zookeeper
dataLogDir=/disk2/zookeeper/logs
clientPort=2181
initLimit=5
syncLimit=2
admin.serverPort=8081
autopurge.snapRetainCount=3
autopurge.purgeInterval=24
"""

    resp = ec2_client.describe_instances(InstanceIds = instance_ids)
    instance_private_ips = []
    instance_public_ips = []
    for i,reservation in enumerate(resp['Reservations']):
        private_ip_addr = reservation['Instances'][0]['PrivateIpAddress']
        public_ip = reservation['Instances'][0]['PublicIpAddress']
        logger.info("Private DNS name of ZooKeeper VM #%d: %s" % (i, private_ip_addr))
        logger.info("Public IP address of ZooKeeper VM #%d: %s" % (i, public_ip))
        instance_private_ips.append(private_ip_addr)
        instance_public_ips.append(public_ip)

    for i,private_ip_addr in enumerate(instance_private_ips):
        config = config + ("server.%d=%s:2888:3888" % (i, private_ip_addr)) + "\n"

    logger.info("Configuration file:\n%s" % str(config))

    key = RSAKey(filename = ssh_key_path)

    for i,ip in enumerate(instance_public_ips):
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        logger.info("Connecting to ZooKeeper VM #%d at %s" % (i, ip))
        ssh_client.connect(hostname = ip, port = 22, username = "ubuntu", pkey = key)
        logger.info("Connected! SFTP-ing configuration now.")
        
        sftp_client = ssh_client.open_sftp()
        file = sftp_client.open("/opt/zookeeper/conf/zoo.cfg", mode = "w")
        
        file.write(config)
        file.close()
        
        zk_env_file = sftp_client.open("/opt/zookeeper/conf/zookeeper-env.sh", mode = "w")
        zk_env_file.write("ZK_SERVER_HEAP=\"%d\"\n" % zookeeper_jvm_heap_size)
        zk_env_file.write("JVMFLAGS=\"-Xms%dm\"\n" % zookeeper_jvm_heap_size)
        zk_env_file.write("ZK_CLIENT_HEAP=\"%d\"\n" % zookeeper_jvm_heap_size)
        zk_env_file.close()
        
        ssh_client.exec_command("echo %d > /data/zookeeper/myid" % i)
        
        ssh_client.close()
    
    data["zk_node_public_IPs"] = instance_public_ips
    data["zk_node_private_ip_addrs"] = instance_private_ips
    
    return instance_public_ips

def start_zookeeper_cluster(
    ips = [],
    ssh_key_path = None,
):
    """
    Start the ZooKeeper cluster.
    """
    if ips == None or len(ips) == 0:
        log_error("Received no ZooKeeper IP addresses. Cannot start the server.")
        return 
    
    if ssh_key_path == None:
        log_error("SSH key path cannot be None.")
        exit(1)
    
    key = RSAKey(filename = ssh_key_path)

    # Start each of the ZooKeeper nodes.
    for ip in ips:
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        logger.info("Connecting to ZooKeeper VM at %s" % ip)
        ssh_client.connect(hostname = ip, port = 22, username = "ubuntu", pkey = key)
        logger.info("Connected! Starting ZooKeeper server now.")
        
        _, stdout, stderr = ssh_client.exec_command("sudo /opt/zookeeper/bin/zkServer.sh start")
        
        logger.info("STDOUT (ZooKeeper VM @ %s): %s" % (ip, stdout.read().decode()))
        logger.info("STDERR (ZooKeeper VM @ %s): %s" % (ip, stderr.read().decode()))
        
        ssh_client.close()
        
def populate_zookeeper(
    ips = [],
    ssh_key_path = None,
):
    """
    SFTP the script used to populate ZooKeeper with data to a ZooKeeper node and then execute it.
    """
    if ips == None or len(ips) == 0:
        log_error("Received no ZooKeeper IP addresses. Cannot start the server.")
        return 
        
    if ssh_key_path == None:
        log_error("SSH key path cannot be None.")
        exit(1)

    target_server_ip = ips[0]
    logger.info("Connecting to ZooKeeper server at %s to populate cluster." % target_server_ip)
    
    key = RSAKey(filename = ssh_key_path)

    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    logger.info("Connecting to ZooKeeper VM at %s" % target_server_ip)
    ssh_client.connect(hostname = target_server_ip, port = 22, username = "ubuntu", pkey = key)
    logger.info("Connected! Populating ZK now.")
    
    sftp = ssh_client.open_sftp()
    sftp.put("./scripts/populate_zk_script", "/home/ubuntu/populate_zk_script")
    
    _, stdout, stderr = ssh_client.exec_command("sudo /opt/zookeeper/bin/zkCli.sh < /home/ubuntu/populate_zk_script")
    
    logger.info("STDOUT (ZooKeeper VM @ %s): %s" % (target_server_ip, stdout.read().decode()))
    logger.info("STDERR (ZooKeeper VM @ %s): %s" % (target_server_ip, stderr.read().decode()))    
    
    ssh_client.close()

def create_ndb_cluster(
    ec2_resource = None,
    ec2_client = None,
    ssh_keypair_name:str = None,
    num_datanodes:int = 4,
    subnet_id:str = None,
    ndb_manager_instance_type:str = "r5.4xlarge",
    ndb_datanode_instance_type:str = "r5.4xlarge",
    ndb_mgm_private_ipv4:str = "10.0.6.227",
    security_group_ids:list = [],
): 
    """
    Create the required AWS infrastructure for the MySQL NDB cluster. 
    
    This includes a total of 5 EC2 VMs: one NDB "master" node and four NDB data nodes.
    
    Returns a dictionary {
        "manager-node-id": the ID of the manager node VM,
        "manager-node-public-ip": public IPv4 of manager node VM,
        "manager-node-private-ip": public IPv4 of manager node VM,
        "data-node-ids": list of IDs of the data node VMs,
        "data-node-public-ips": list of public IPv4s of the data node VMs,
        "data-node-private-ips": list of private IPv4s of the data node VMs
    }
    """
    if ec2_resource == None:
        log_error("EC2 resource cannot be null when creating the NDB cluster.")
        exit(1)
    
    if ssh_keypair_name == None:
        log_error("SSH keypair name cannot be null when creating the NDB cluster.")
        exit(1)
        
    
    logger.info("Creating 1 MySQL NDB Manager Node.")
    
    # Create the NDB manager server.
    ndb_manager_instance = ec2_resource.create_instances(
        MinCount = 1,
        MaxCount = 1,
        ImageId = MYSQL_NDB_MANAGER_AMI,
        InstanceType = ndb_manager_instance_type,
        KeyName = ssh_keypair_name,
        NetworkInterfaces = [{
            "AssociatePublicIpAddress": True,
            "DeleteOnTermination": True,
            "PrivateIpAddress": ndb_mgm_private_ipv4,
            "DeviceIndex": 0,
            "SubnetId": subnet_id,
                "Groups": security_group_ids
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{
                        'Key': 'Name',
                        'Value': "ndb-manager-node"
                    },
                    {
                        'Key': 'Project',
                        'Value': 'LambdaFS'
                    },
                    {
                        'Key': 'Component',
                        'Value': Component_NDB_ManagerVM
                    }]
        }]  
    )
    
    num_type_1_datanodes = num_datanodes // 2
    if num_datanodes % 2 == 0:
        num_type_2_datanodes = num_type_1_datanodes
    else:
        num_type_2_datanodes = num_type_1_datanodes+1 
        
    logger.info("Creating %d type 1 NDB data node(s) and %d type 2 NDB data node(s)." % (num_type_1_datanodes, num_type_2_datanodes))
    
    type1_datanodes = []
    type2_datanodes = []
    datanode_ids = [] 
    datnaode_private_ips = []
    
    logger.info("Creating %d Type 1 MySQL NDB Data Node(s)." % num_type_1_datanodes)
    
    ndb_datanode_index = 0
    for _ in range(0, num_type_1_datanodes):
        instance_name = "ndb-datanode-type1-%d" % ndb_datanode_index
        ndb_datanode_index += 1
        # Create `num_datanodes` NDB data nodes.
        type1_datanode = ec2_resource.create_instances(
            MinCount = 1,
            MaxCount = 1,
            ImageId = MYSQL_NDB_DATANODE1_AMI,
            InstanceType = ndb_datanode_instance_type,
            KeyName = ssh_keypair_name,
            NetworkInterfaces = [{
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "SubnetId": subnet_id,
                "Groups": security_group_ids
            }],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': 
                    [{
                        'Key': 'Name',
                        'Value': instance_name
                    },
                    {
                        'Key': 'Project',
                        'Value': 'LambdaFS'
                    },
                    {
                        'Key': 'Component',
                        'Value': Component_NDB_DataNodeVM
                    }]
            }]   
        ) # end of call to ec2_client.create_instances()
        type1_datanodes.append(type1_datanode[0].id)
        datanode_ids.append(type1_datanode[0].id)
        datnaode_private_ips.append(type1_datanode[0].private_ip_address)
    
    logger.info("Creating %d Type 2 MySQL NDB Data Node(s)." % num_type_2_datanodes)
    
    for _ in range(0, num_type_2_datanodes):
        instance_name = "ndb-datanode-type2-%d" % ndb_datanode_index
        ndb_datanode_index += 1
        type2_datanode = ec2_resource.create_instances(
            MinCount = 1,
            MaxCount = 1,
            ImageId = MYSQL_NDB_DATANODE2_AMI,
            InstanceType = ndb_datanode_instance_type,
            KeyName = ssh_keypair_name,
            NetworkInterfaces = [{
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "SubnetId": subnet_id,
                "Groups": security_group_ids
            }],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': 
                    [{
                        'Key': 'Name',
                        'Value': instance_name
                    },
                    {
                        'Key': 'Project',
                        'Value': 'LambdaFS'
                    },
                    {
                        'Key': 'Component',
                        'Value': Component_NDB_DataNodeVM
                    }]
            }], 
        )
        type2_datanodes.append(type2_datanode[0].id)
        datanode_ids.append(type2_datanode[0].id)
        datnaode_private_ips.append(type2_datanode[0].private_ip_address)
    
    logger.info("Created NDB EC2 instances.")
    logger.info("Created 1 NDB Manager Node and %d NDB DataNode(s)." % len(datanode_ids))
    logger.info("Sleeping for 60 seconds while the NDB VMs start-up.")
    for i in tqdm(range(240)):
        sleep(0.25)
    
    # Resolving this separately/explicitly, as it kept being None when I tried to access the field, even after waiting.
    ndb_mgm_public_ip = ndb_manager_instance[0].public_ip_address
    if ndb_mgm_public_ip == None:
        ndb_mgm_desc_resp = ec2_client.describe_instances(InstanceIds = [ndb_manager_instance[0].id])
        ndb_mgm_public_ip = ndb_mgm_desc_resp['Reservations'][0]['Instances'][0]['PublicIpAddress']
        
        if ndb_mgm_public_ip == None:
            log_error("Cannot resolve MySQL Cluster NDB manager node public IPv4.")
            log_error("Terminating NDB manager node.")
            ec2_client.terminate_instances(InstanceIds = [ndb_manager_instance[0].id])
            log_error("Terminating the %d NDB data node(s)." % len(datanode_ids))
            ec2_client.terminate_instances(InstanceIds = datanode_ids)
            
            exit(1)
        
        logger.debug("IPv4 of NDB manager node: %s" % ndb_mgm_public_ip)
    
    datanode_public_ips = []
    ndb_dn_resp = ec2_client.describe_instances(InstanceIds = datanode_ids)
    for i,reservation in enumerate(ndb_dn_resp["Reservations"]):
        dn_public_ip = reservation['Instances'][0]['PublicIpAddress']
        datanode_public_ips.append(dn_public_ip)
        logger.debug("IPv4 of NDB data node #%d: %s" % (i, dn_public_ip))
    
    return {
        "manager-node-id": ndb_manager_instance[0].id,
        "manager-node-public-ip": ndb_mgm_public_ip,
        "manager-node-private-ip": ndb_manager_instance[0].private_ip_address,
        "data-node-ids": datanode_ids,
        "data-node-public-ips": datanode_public_ips,
        "data-node-private-ips": datnaode_private_ips
    }

def create_ndb_config(
    ndb_mgm_public_ip:str = None,
    ndb_mgm_private_ip:str = None,
    ssh_key_path = None,
    data_node_public_ips:list[str] = None,
    data_node_private_ips:list[str] = None,
    ndb_mgm_data_directory:str = "/var/lib/mysql-cluster",
    ndb_datanode_data_directory:str = "/usr/local/mysql/data"
):
    """
    Create/update the configuration of the MySQL NDB manager node.
    """
    """
    SFTP the script used to populate ZooKeeper with data to a ZooKeeper node and then execute it.
    """
    if ndb_mgm_public_ip == None:
        log_error("Received no NDB manager node public IP address. Cannot configure the cluster.")
        return 

    if ndb_mgm_private_ip == None:
        log_error("Received no NDB manager node private IP address. Cannot configure the cluster.")
        return 
        
    if ssh_key_path == None:
        log_error("SSH key path cannot be None.")
        exit(1)
        
    key = RSAKey(filename = ssh_key_path)
    
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    logger.info("Connecting to MySQL NDB Manager Node VM at %s" % ndb_mgm_public_ip)
    ssh_client.connect(hostname = ndb_mgm_public_ip, port = 22, username = "ubuntu", pkey = key)
    logger.info("Connected! SFTP-ing base config file now.")
    
    sftp = ssh_client.open_sftp()
    
    if not os.path.exists("./temporary"):
        os.makedirs("./temporary")
        
    shutil.copyfile("./ndb_configs/ndb_config.ini", "./temporary/config.ini")
        
    next_node_id = 1
    # Write the [ndb_mgmd] portion of the config now.
    with open("./temporary/config.ini", "a", newline = "") as local_ndb_mgm_config_file:
        local_ndb_mgm_config_file.write("[ndb_mgmd]\n")
        local_ndb_mgm_config_file.write("# Management process options:\n")
        local_ndb_mgm_config_file.write("HostName=%s          # Hostname or IP address of management node\n" % ndb_mgm_private_ip)
        local_ndb_mgm_config_file.write("DataDir=%s  # Directory for management node log files\n" % ndb_mgm_data_directory)
        local_ndb_mgm_config_file.write("NodeId=%d\n\n" % next_node_id)
        next_node_id += 1
    
        # Write the [ndbd] portions of the config now.
        for data_node_private_ip in data_node_private_ips:
            local_ndb_mgm_config_file.write("[ndbd]\n")
            local_ndb_mgm_config_file.write("HostName=%s          # Hostname or IP address of management node\n" % data_node_private_ip)
            local_ndb_mgm_config_file.write("NodeId=%d\n" % next_node_id)
            local_ndb_mgm_config_file.write("DataDir=%s  # Directory for management node log files\n\n" % ndb_datanode_data_directory)
            next_node_id += 1

        # There can be 255 nodes. Configure the cluster to allow for this number of nodes in total by adding the appropriate number of [api] tags.
        # This is calculated by subtracting from 255 the value (1 + NumDataNodes), where the 1 is for the NDB manager server.
        num_api_nodes = 255 - (1 + len(data_node_public_ips))
        
        for _ in range(0, num_api_nodes):
            local_ndb_mgm_config_file.write("[api]\n")
    
    # Copy over the file we just created using SFTP.
    sftp.put("./temporary/config.ini", "/var/lib/mysql-cluster/config.ini")
    os.remove("./temporary/config.ini")
    
    # Create the /etc/my.cnf configuration file for the manager node.
    with open("./temporary/my.cnf", "w", newline = "") as local_ndb_my_cnf:
        local_ndb_my_cnf.write("[mysqld]\n")
        local_ndb_my_cnf.write("# Options for mysqld process:\n")
        local_ndb_my_cnf.write("ndbcluster                      # run NDB storage engine\n")
        local_ndb_my_cnf.write("\n")
        local_ndb_my_cnf.write("[mysql_cluster]\n")
        local_ndb_my_cnf.write("# Options for NDB Cluster processes:\n")
        local_ndb_my_cnf.write("ndb-connectstring=%s  # location of management server\n" % ndb_mgm_private_ip)
    
    sftp.put("./temporary/my.cnf", "/etc/my.cnf")
    ssh_client.close()
    
    # TODO: The /etc/my.cnf configuration file for the data nodes.
    for dn_public_ip in data_node_public_ips:
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        logger.info("Connecting to MySQL NDB data node VM at %s" % dn_public_ip)
        ssh_client.connect(hostname = dn_public_ip, port = 22, username = "ubuntu", pkey = key)
        sftp = ssh_client.open_sftp()
        sftp.put("./temporary/my.cnf", "/etc/my.cnf")
        ssh_client.close()

    os.remove("./temporary/my.cnf")
    
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    logger.info("Connecting to MySQL NDB Manager Node VM at %s" % ndb_mgm_public_ip)
    ssh_client.connect(hostname = ndb_mgm_public_ip, port = 22, username = "ubuntu", pkey = key)
    logger.info("Connected! SFTP-ing base config file now.")
    
    sftp = ssh_client.open_sftp()
    sftp.mkdir("/home/ubuntu/mysql_scripts")
    
    # Copy over .sql scripts.
    for entry in os.scandir("./sql_scripts"):
        if entry.path.endswith(".sql"):
            filename = entry.name
            dest_path = "/home/ubuntu/mysql_scripts/%s" % filename
            logger.debug("Copying SQL script \"%s\". Local: %s. Remote: %s." % (entry.name, entry.path, dest_path))
            sftp.put(entry.path, dest_path)
    
    ssh_client.close()

def stop_mysqld_process(
    ip:str = None,
    key:RSAKey = None,
)->bool:
    if ip == None:
        log_error("IP cannot be None.")
        return 
        
    if key == None:
        log_error("SSH key cannot be None.")
        return 

    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    logger.info("Connecting to NDB VM at %s" % ip)
    ssh_client.connect(hostname = ip, port = 22, username = "ubuntu", pkey = key)
    logger.info("Connected! Stopping mysqld process first.")
    _, stdout, stderr = ssh_client.exec_command(STOP_MYSQLD_COMMAND)
    logger.info(stdout.read().decode())
    stderr_output = stderr.read().decode()

    if len(stderr_output.strip()) > 0:
        log_error(stderr_output)
        
        if "ERROR" in stderr_output.upper() or "EXCEPTION" in stderr_output.upper():
            return False 
    
    return True 

def start_ndb_cluster(
    ndb_mgm_ip:str = None,
    data_node_ips:list[str] = None,
    ssh_key_path:str = None,
    start_manager_node:bool = True,
    #first_start:bool = False # If True, then we start the Cluster as if it is a brand new cluster.
):
    """
    Start the MySQL NDB cluster. 
    """
    if ndb_mgm_ip == None:
        log_error("Received no NDB manager node IP address. Cannot start the cluster.")
        return False

    if data_node_ips == None or len(data_node_ips) == 0:
        log_error("Received no NDB data node IP addresses. Cannot start the cluster.")
        return False
        
    if ssh_key_path == None:
        log_error("SSH key path cannot be None.")
        return False
    
    key = RSAKey(filename = ssh_key_path)
    
    if start_manager_node:
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        logger.info("Connecting to MySQL NDB Manager Node VM at %s" % ndb_mgm_ip)
        ssh_client.connect(hostname = ndb_mgm_ip, port = 22, username = "ubuntu", pkey = key)
        logger.info("Connected! Starting manager now.")
    
        _, stdout, stderr = ssh_client.exec_command("sudo -S /usr/local/bin/ndb_mgmd --initial --skip-config-cache -f /var/lib/mysql-cluster/config.ini")    
        logger.info(stdout.read().decode())
        stderr_output = stderr.read().decode()
        
        if len(stderr_output.strip()) > 0:
            log_error(stderr_output)
            
            if "ERROR" in stderr_output.upper() or "EXCEPTION" in stderr_output.upper():
                log_error("Exiting. The NDB EC2 VMs will NOT be terminated, though. Please visit the AWS EC2 Console to terminate the VMs (or use the command-line).")
                # TODO: Terminate NDB EC2 VMs at this point?
                return False
    
        log_success("Started NDB manager node.")
        ssh_client.close()
    
    print()
    logger.info("Starting data nodes.")
    
    for i in tqdm(range(len(data_node_ips))):
        data_node_ip = data_node_ips[i]
        
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        logger.info("Connecting to MySQL NDB Data Node VM at %s" % data_node_ip)
        ssh_client.connect(hostname = data_node_ip, port = 22, username = "ubuntu", pkey = key)
        logger.info("Connected! Starting data node now.")

        _, stdout, stderr = ssh_client.exec_command("sudo -S /usr/local/bin/ndbmtd --initial")    
        logger.info(stdout.read().decode())
        logger.info(stderr.read().decode())

        ssh_client.close()

        logger.info("Started data node (hopefully).")

    logger.info("Sleeping for 30 seconds before starting mysqld process on NDB manager node.")
    for _ in tqdm(range(120)):
        sleep(0.25)

    logger.info("Restarting mysql service on NDB manager node. (This may take a minute or two.)")
    
    # Restart mysqld on manager node.
    start_mysqld_process(ip = ndb_mgm_ip, key = key)
    
    # Restart mysqld on each data node.
    # for i in tqdm(range(len(data_node_ips))):
    #     data_node_ip = data_node_ips[i]

    #     start_mysqld_process(ip = data_node_ip, key = key)
    
    return True 

def start_mysqld_process(
    ip:str = None,
    key:RSAKey = None,
)->bool:
    if ip == None:
        log_error("IP cannot be None.")
        return False
        
    if key == None:
        log_error("SSH key cannot be None.")
        return False

    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    logger.info("Connecting to NDB VM at %s" % ip)
    ssh_client.connect(hostname = ip, port = 22, username = "ubuntu", pkey = key)
    logger.info("Connected! Restarting mysql service now with command \"%s\"" % START_MYSQLD_COMMAND)
    
    st = time.time() 
    try:
        _, stdout, stderr = ssh_client.exec_command(START_MYSQLD_COMMAND, timeout = 30)    
        logger.info(stdout.read().decode())
        logger.info(stderr.read().decode())
    except Exception:
        logger.info("Starting mysqld did not return within 30 seconds. Continuing with the assumption that it started correctly...")
        pass
    
    logger.info("Restarted MySQL service on VM at %s. Time elapsed: %.2f seconds." % (ip, time.time() - st))

    logger.info("Sleeping for 30 seconds so the MYSQLD process has time to start-up.")
    for _ in tqdm(range(180)):
        sleep(0.25)
            
    return True 

def populate_mysql_ndb_tables(
    ndb_mgm_ip:str = None,
    ssh_key_path:str = None,
    create_user:bool = True,
)->bool:
    """
    Populate the MySQL Cluster NDB tables.
    """
    if ndb_mgm_ip == None:
        log_error("Received no NDB manager node IP address. Cannot start the cluster.")
        return False
        
    if ssh_key_path == None:
        log_error("SSH key path cannot be None.")
        return False
    
    key = RSAKey(filename = ssh_key_path)

    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    logger.info("Connecting to MySQL NDB Manager Node VM at %s" % ndb_mgm_ip)
    ssh_client.connect(hostname = ndb_mgm_ip, port = 22, username = "ubuntu", pkey = key)
    logger.info("Connected to manager node.")
    
    if create_user:
        # Create MySQL user.
        logger.info("Creating MySQL user \"user\"")
        
        _, stdout, stderr = ssh_client.exec_command("cd /home/ubuntu/mysql_scripts; sudo -S /usr/local/mysql/bin/mysql --host=localhost --port=22 vanilla_hopsfs < create_user.sql")  
        
        logger.info(stdout.read().decode())
        stderr_output = stderr.read().decode()
        
        if len(stderr_output.strip()) > 0:
            log_error(stderr_output)
            
            if "ERROR" in stderr_output.upper():
                log_error("Exiting. The NDB EC2 VMs will NOT be terminated, though. Please visit the AWS EC2 Console to terminate the VMs (or use the command-line).")
                exit(1)
        
        log_success("Created MySQL user \"user\"")
    
    # logger.debug("Executing MySQL script: \"schema.sql\"")
    
    mysql_scripts = [
        "schema.sql",
        "update-schema_2.8.2.1_to_2.8.2.2.sql",
        "update-schema_2.8.2.2_to_2.8.2.3.sql",
        "update-schema_2.8.2.3_to_2.8.2.4.sql",
        "update-schema_2.8.2.4_to_2.8.2.5.sql",
        "update-schema_2.8.2.5_to_2.8.2.6.sql",
        "update-schema_2.8.2.6_to_2.8.2.7.sql",
        "update-schema_2.8.2.7_to_2.8.2.8.sql",
        "update-schema_2.8.2.8_to_2.8.2.9.sql",
        "update-schema_2.8.2.9_to_2.8.2.10.sql",
        "update-schema_2.8.2.10_to_3.2.0.0.sql",
        "serverless.sql",
    ]
    
    logger.info("Executing %d MySQL scripts now." % len(mysql_scripts))
    
    for mysql_script in mysql_scripts:
        logger.debug("Executing MySQL script: \"%s\"" % mysql_script)
    
        # Execute MySQL commands to populate tables/database.
        _, stdout, stderr = ssh_client.exec_command("cd /home/ubuntu/mysql_scripts; sudo /usr/local/mysql/bin/mysql --host=localhost --port=22 -u user -p123password123 vanilla_hopsfs < %s" % mysql_script)  
        logger.info(stdout.read().decode())

        stderr_output = stderr.read().decode()
        
        if len(stderr_output.strip()) > 0:
            log_error(stderr_output)
            
            if "ERROR" in stderr_output.upper() or "EXCEPTION" in stderr_output.upper():
                log_error("Exiting.")
                return False
        
    log_success("Executed %d MySQL scripts." % len(mysql_scripts))
    
    ssh_client.close()
    
    return True 

def ndb_part(
    do_create_ndb_cluster:bool = True,
    do_start_ndb_cluster:bool = True,
    do_populate_mysql_ndb_tables:bool = True,
    ndb_datanode_data_directory:str = "/usr/local/mysql/data",
    ndb_mgm_data_directory:str = "/var/lib/mysql-cluster",
    num_ndb_datanodes:int = 4,
    security_group_ids:list[str] = [],
    public_subnet_ids:list[str] = [],
    ndb_manager_instance_type:str = "r5.4xlarge",
    ndb_datanode_instance_type:str = "r5.4xlarge",
    ndb_mgm_private_ipv4:str = "10.0.6.227",
    ssh_keypair_name:str = None,
    ssh_key_path:str = None,
    ec2_resource = None,
    ec2_client = None,  
    data:dict = None ,
    infrastructure_json = None,
    current_datetime = None
):
    """
    Setup MySQL Cluster NDB. 
    
    This may include:
    - Creating the EC2 VMs for the NDB Manager Node and NDB Data Node(s).
    - Updating the configuration for the various NDB nodes.
    - Starting the NDB cluster.
    - Populating the cluster with the SQL tables required by λFS and HopsFS.
    """
    ndb_mgm_public_ip = None
    data_node_public_ips = None 
    datanode_ids = None
    ndb_manager_node_id = None
    if do_create_ndb_cluster:
        logger.info("Creating the MySQL NDB cluster nodes now.")
        ndb_resp = create_ndb_cluster(
            ec2_resource = ec2_resource, 
            ec2_client = ec2_client,
            ssh_keypair_name = ssh_keypair_name, 
            num_datanodes = num_ndb_datanodes, 
            security_group_ids = security_group_ids,
            subnet_id = public_subnet_ids[1],
            ndb_mgm_private_ipv4 = ndb_mgm_private_ipv4,
            ndb_manager_instance_type = ndb_manager_instance_type,
            ndb_datanode_instance_type = ndb_datanode_instance_type)
        
        log_success("Created NDB Manager Node: %s" % ndb_resp["manager-node-id"])
        log_success("Created %d NDB Data Node(s): %s" % (len(ndb_resp["data-node-ids"]), str(ndb_resp["data-node-ids"])))
        
        data.update(ndb_resp)
        
        ndb_mgm_public_ip = ndb_resp["manager-node-public-ip"]
        ndb_mgm_private_ip = ndb_resp["manager-node-private-ip"]
        data_node_public_ips = ndb_resp["data-node-public-ips"]
        data_node_private_ips = ndb_resp["data-node-private-ips"]
        datanode_ids = ndb_resp["data-node-ids"]
        ndb_manager_node_id = ndb_resp["manager-node-id"]
            
        # key = RSAKey(filename = ssh_key_path)
        # Make sure the mysqld process is stopped on the manager node.
        # success = stop_mysqld_process(ip = ndb_mgm_public_ip, key = key)
        # if not success:
        #     log_error("Failed to stop MYSQLD process on NDB manager node.")
        #     log_error("Exiting. The NDB EC2 VMs will NOT be terminated, though. Please visit the AWS EC2 Console to terminate the VMs (or use the command-line).")
        #     exit(1)

        # # Make sure the mysqld process is stopped on the data nodes.
        # for i in tqdm(range(len(data_node_public_ips))):
        #     data_node_ip = data_node_public_ips[i]

        #     success = stop_mysqld_process(ip = data_node_ip, key = key)
        #     if not success:
        #         log_error("Failed to stop MYSQLD process on NDB data node at %s." % data_node_ip)
        #         log_error("Exiting. The NDB EC2 VMs will NOT be terminated, though. Please visit the AWS EC2 Console to terminate the VMs (or use the command-line).")
        #         exit(1)
        
        try:
            print()
            logger.info("Creating/updating NDB configuration now.")
            create_ndb_config(ndb_mgm_public_ip = ndb_mgm_public_ip, ndb_mgm_private_ip = ndb_mgm_private_ip, ssh_key_path = ssh_key_path, ndb_datanode_data_directory = ndb_datanode_data_directory, ndb_mgm_data_directory = ndb_mgm_data_directory, data_node_public_ips = data_node_public_ips, data_node_private_ips = data_node_private_ips)
        except Exception as ex:
            log_error("Exception encountered while creating NDB configuration files.")
            log_error(repr(ex))
            log_error("Terminating NDB manager node.")
            ec2_client.terminate_instances(InstanceIds = [ndb_manager_node_id])
            log_error("Terminating the %d NDB data node(s)." % len(datanode_ids))
            ec2_client.terminate_instances(InstanceIds = datanode_ids)
            raise ex 
   
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    if do_start_ndb_cluster:
        if ndb_mgm_public_ip == None:
            if "manager-node-public-ip" not in infrastructure_json:
                log_error("We did not just create the MySQL NDB cluster; however, the `infrastructure_json` data does not have an entry for the NDB manager node's public IP address.")
                log_error("Consequently, we cannot start the NDB cluster as instructed.")
                exit(1)
            
            logger.debug("Retrieving NDB manager node's IP from the `infrastructure_json` data.")
            ndb_mgm_public_ip = infrastructure_json.get("manager-node-public-ip", None)
            ndb_manager_node_id = infrastructure_json.get("manager-node-id", None)
        
        if data_node_public_ips == None:
            if "data-node-public-ips" not in infrastructure_json:
                log_error("We did not just create the MySQL NDB cluster; however, the `infrastructure_json` data does not have an entry for the NDB data nodes' public IP address.")
                log_error("Consequently, we cannot start the NDB cluster as instructed.")
                exit(1)
            
            logger.debug("Retrieving NDB data nodes' IP from the `infrastructure_json` data.")
            data_node_public_ips = infrastructure_json.get("data-node-public-ips", None)
            datanode_ids = infrastructure_json.get("data-node-ids", None)
        
        # logger.info("Sleeping for 30 seconds while the mysqld processes shut down.")
        # for _ in tqdm(range(120)):
        #     sleep(0.25)
        
        print()
        logger.info("Starting the MySQL NDB cluster now.")
        success = start_ndb_cluster(ndb_mgm_ip = ndb_mgm_public_ip, ssh_key_path = ssh_key_path, data_node_ips = data_node_public_ips)
        
        if success:
            log_success("Successfully started the MySQL NDB cluster (hopefully).")
        else:
            log_error("Failed to start the MySQL NDB cluster.")
            log_error("Terminating NDB manager node.")
            ec2_client.terminate_instances(InstanceIds = [ndb_manager_node_id])
            log_error("Terminating the %d NDB data node(s)." % len(datanode_ids))
            ec2_client.terminate_instances(InstanceIds = datanode_ids)
        
        logger.info("Sleeping for 60 seconds while the MySQL NDB Cluster begins running.")
        for _ in tqdm(range(240)):
            sleep(0.25)

    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    if do_populate_mysql_ndb_tables:
        if ndb_mgm_public_ip == None:
            if "manager-node-public-ip" not in infrastructure_json:
                log_error("We did not just create the MySQL NDB cluster; however, the `infrastructure_json` data does not have an entry for the NDB manager node's public IP address.")
                log_error("Consequently, we cannot start the NDB cluster as instructed.")
                exit(1)
            
            logger.debug("Retrieving NDB manager node's IP from the `infrastructure_json` data.")
            ndb_mgm_public_ip = infrastructure_json.get("manager-node-public-ip", None)
            ndb_manager_node_id = infrastructure_json.get("manager-node-id", None)
        
        print()
        logger.info("Populating the MySQL NDB database now.")
        success = populate_mysql_ndb_tables(ndb_mgm_ip = ndb_mgm_public_ip, ssh_key_path = ssh_key_path)
        
        if success:
            log_success("Successfully populated the MySQL NDB cluster.")
        else:
            log_error("Failed to populate MySQL cluster with tables. See log output for relevant error messages, if any exist.")
            exit(1)
    
def get_args() -> argparse.Namespace:
    """
    Parse the commandline arguments.
    """
    parser = argparse.ArgumentParser()
    
    # YAML
    parser.add_argument("-y", "--yaml", type = str, default = "config_aws.yaml", help = "The path of a YAML configuration file.") #, which can be used in-place of command-line arguments. If nothing is passed for this, then command-line arguments will be used. If a YAML file is passed, then command-line arguments for properties that CAN be defined in YAML will be ignored (even if you did not define them in the YAML file).")
    return parser.parse_args()

def main():
    global NO_COLOR
    
    start_time = time.time()
    
    current_datetime = str(datetime.now())
    current_datetime = current_datetime.replace(":", "_")
    
    command_line_args = get_args() 
    
    log_success("Welcome to the λFS Interactive Setup.")
    log_warning("Before you continue, please note that many of the components required by λFS (and HopsFS) cost money.")
    log_warning("AWS will begin charging you for these resources as soon as they are created.")
    print()
    print()
    
    using_yaml = False 
    
    # Keep track of instance IDs and whatnot so we can find them later.
    data = dict() 
    
    if command_line_args.yaml is not None:
        using_yaml = True 
        with open(command_line_args.yaml, "r") as stream:
            logger.info("Loading arguments from YAML file located at \"%s\"" % command_line_args.yaml)
            try:
                arguments = yaml.safe_load(stream)
                log_success("Loaded %s arguments from YAML file." % len(arguments))
            except yaml.YAMLError as exc:
                log_error("Failed to load arguments from YAML file \"%s\"." % command_line_args.yaml)
                log_error("Error: %s" % str(exc))
                exit(1) 
            
            NO_COLOR = arguments.get("no_color", False)
            aws_profile_name = arguments.get("aws_profile", None)
            aws_region = arguments.get("aws_region", "us-east-1")
            user_public_ip = arguments.get("user_public_ip", "DEFAULT_VALUE")
            vpc_name = arguments.get("vpc_name", "LambdaFS_VPC")
            vpc_cidr_block = "10.0.0.0/16" 
            
            security_group_name = arguments.get("security_group_name", "lambda-fs-security-group")
            eks_cluster_name = arguments.get("eks_cluster_name ", "lambda-fs-eks-cluster")
            eks_iam_role_name = arguments.get("eks_iam_role_name", "lambda-fs-eks-cluster-role")
            
            ssh_keypair_name = arguments.get("ssh_keypair_name", None)
            ssh_key_path = arguments.get("ssh_key_path", None)
            
            num_ndb_datanodes = arguments.get("num_ndb_datanodes", 4)
            ndb_mgm_data_directory = arguments.get("ndb_mgm_data_directory", "/var/lib/mysql-cluster")
            ndb_datanode_data_directory = arguments.get("ndb_datanode_data_directory", "/usr/local/mysql/data")

            if ndb_mgm_data_directory != "/var/lib/mysql-cluster":
                log_warning("You specified non-default NDB MGM data directory of \"%s\"." % ndb_mgm_data_directory)
                log_warning("The default NDB MGM data directory is \"/var/lib/mysql-cluster\".")
                log_warning("Changing this value can break the NDB deployment unless you are careful and know what you're doing.")
            
            if ndb_datanode_data_directory != "/usr/local/mysql/data":
                log_warning("You specified non-default NDB datanode data directory of \"%s\"." % ndb_datanode_data_directory)
                log_warning("The default NDB datanode data directory is \"/usr/local/mysql/data\".")
                log_warning("Changing this value can break the NDB deployment unless you are careful and know what you're doing.")
            
            num_lambda_fs_zk_vms = arguments.get("num_lambda_fs_zk_vms", 3)
            
            lfs_client_ags_it = arguments.get("lfs_client_autoscaling_group_instance_type", "r5.4xlarge")
            hopsfs_client_ags_it = arguments.get("hopsfs_client_autoscaling_group_instance_type", "r5.4xlarge")
            hopsfs_namenode_ags_it = arguments.get("hopsfs_namenode_autoscaling_group_instance_type", "r5.4xlarge")
            lfs_client_vm_instance_type = arguments.get("lfs_client_vm_instance_type", "r5.4xlarge")
            ndb_manager_instance_type = arguments.get("ndb_manager_instance_type", "r5.4xlarge")
            ndb_datanode_instance_type = arguments.get("ndb_datanode_instance_type", "r5.4xlarge")
            lambdafs_zk_instance_type = arguments.get("lambdafs_zk_instance_type", "r5.4xlarge")
            hopsfs_client_vm_instance_type = arguments.get("hopsfs_client_vm_instance_type", "r5.4xlarge")
            
            do_create_lambda_fs_client_vm = arguments.get("create_lambda_fs_client_vm", True)
            do_create_hops_fs_client_vm = arguments.get("create_hops_fs_client_vm", True)
            do_start_zookeeper_cluster = arguments.get("start_zookeeper_cluster", True)
            do_populate_zookeeper_cluster = arguments.get("populate_zookeeper_cluster", True)
            create_zookeeper_vms = arguments.get("create_zookeeper_vms", True)
            
            do_create_ndb_cluster = arguments.get("create_ndb_cluster", True)
            do_start_ndb_cluster = arguments.get("start_ndb_cluster", True)
            do_populate_mysql_ndb_tables = arguments.get("populate_mysql_ndb_tables", True)
            
            # If you change this, you'll have to rebuild and reconfigure the λFS NameNode image, as
            # it is configured by-default to expect the NDB manager node to have the IP address 10.0.6.227.
            ndb_mgm_private_ipv4 = arguments.get("ndb_mgm_private_ipv4", "10.0.6.227")
            
            skip_iam_role_creation = arguments.get("skip_iam_role_creation", False)
            skip_vpc_creation = arguments.get("skip_vpc_creation", False)
            skip_eks = arguments.get("skip_eks", False)
            skip_launch_templates = arguments.get("skip_launch_templates", False)
            skip_autoscaling_groups = arguments.get("skip_autoscaling_groups", False)
            
            zookeeper_jvm_heap_size = arguments.get("zookeeper_jvm_heap_size", 4000)
            
            infrastructure_json_path = arguments.get("infrastructure_json_path", None)
            infrastructure_json = None 
            if infrastructure_json_path is not None:
                logger.info("Loading existing infrastructure information from JSON file at \"%s\" now." % infrastructure_json_path)
                with open(infrastructure_json_path, "r") as infrastructure_json_file:
                    infrastructure_json = json.load(infrastructure_json_file)
                
                log_success("Loaded existing infrastructure:")
                logger.info(str(infrastructure_json))
                
                data.update(infrastructure_json)
            
            # start_zookeeper = arguments.get("start_zoo_keeper", False)
            # start_ndb = arguments.get("start_ndb", False)
            
            if ssh_key_path == None and (do_create_ndb_cluster or create_zookeeper_vms or do_create_lambda_fs_client_vm or do_create_hops_fs_client_vm):
                log_error("The SSH key path cannot be None.")
                exit(1)
                
            if ssh_keypair_name == None and (do_create_ndb_cluster or create_zookeeper_vms or do_create_lambda_fs_client_vm or do_create_hops_fs_client_vm):
                log_error("The SSH keypair name cannot be None.")
                exit(1)
    else:
        log_error("Please specify the path to the YAML configuration file.")
        exit(1) 
    
    if user_public_ip == "DEFAULT_VALUE":
        log_warning("Attempting to resolve your IP address automatically...")
        try:
            user_public_ip = get('https://api.ipify.org', timeout = 5).content.decode('utf8')
            log_success("Successfully resolved your IP address.")
            print()
        except (requests.exceptions.ReadTimeout, urllib3.exceptions.ReadTimeoutError):
            log_error("Could not connect to api.ipify.org to resolve your IP address. Please pass your IP address to this script directly to continue.")
            exit(1)
    
    try:
        socket.inet_aton(user_public_ip)
    except OSError:
        log_error("Invalid user IP address: \"%s\"" % user_public_ip)
        exit(1) 
    
    if aws_profile_name == None:
        log_warning("AWS profile == None.")
        log_warning("If you are unsure what profile to use, you can list the available profiles on your device via the 'aws configure list-profiles' command.")
        log_warning("Execute that command ('aws configure list-profiles') in a terminal or command prompt session to view a list of the available AWS CLI profiles.")
        log_warning("For now, we will use the default profile.")
    else:
        logger.info("Selected AWS profile: \"%s\"" % aws_profile_name)

    print()
    print()
    print()
    log_important("Please verify that the following information is correct before continuing.")
    print()
    
    if using_yaml:
        for arg_name, arg_value in arguments.items():
            logger.info("{:50s}= \"{}\"".format(arg_name, str(arg_value)))
        
        logger.info("{:50s}= \"{}\"".format("NDB manager private IPv4", str(ndb_mgm_private_ipv4)))
    else:
        for arg in vars(command_line_args):
            logger.info("{:50s}= \"{}\"".format(arg, str(getattr(command_line_args, arg))))
    
    # Give the user a chance to verify that the information they specified is correct.
    while True:
        print()
        logger.info("Proceed? [y/n]")
        proceed = input(">")
        if proceed.strip().lower() == "y" or proceed.strip().lower() == "yes":
            print() 
            log_important("Continuing.")
            print()
            break 
        elif proceed.strip().lower() == "n" or proceed.strip().lower() == "no":
            log_important("User elected not to continue. This script will now terminate.")
            exit(0)
        else:
            log_error("Please enter \"y\" for yes or \"n\" for no. You entered: \"%s\"" % proceed)
    
    session:boto3.Session = None 
    if aws_profile_name is not None:
        logger.info("Attempting to create AWS Session using explicitly-specified credentials profile \"%s\" now..." % aws_profile_name)
        try:
            session = boto3.Session(profile_name = aws_profile_name)
            log_success("Successfully created boto3 Session using AWS profile \"%s\"" % aws_profile_name)
        except Exception as ex: 
            log_error("Exception encountered while trying to use AWS credentials profile \"%s\"." % aws_profile_name)
            raise ex 
        ec2_client = session.client('ec2', region_name = aws_region)
        ec2_resource = session.resource('ec2', region_name = aws_region)
        autoscaling_client = session.client("autoscaling", region_name = aws_region)
    else:
        ec2_client = boto3.client('ec2', region_name = aws_region)
        ec2_resource = boto3.resource('ec2', region_name = aws_region)
        autoscaling_client = boto3.client("autoscaling", region_name = aws_region)

    if not validate_keypair_exists(ssh_keypair_name = ssh_keypair_name, ec2_client = ec2_client):
        log_error("Could not find SSH keypair named \"%s\" registered with AWS." % ssh_keypair_name)
        log_error("Please verify that the given keypair exists, is registered with AWS, and has no typos in its name.")
        exit(1)

    data["aws_region"] = aws_region 
    data["user_public_ip"] = user_public_ip
    data["vpc_name"] = vpc_name 

    vpc_id:str = None 
    if not skip_vpc_creation:
        logger.info("Creating Virtual Private Cloud now.")
        vpc_id = create_vpc(
            aws_region = aws_region,
            vpc_name = vpc_name, 
            vpc_cidr_block = vpc_cidr_block, 
            security_group_name = security_group_name,
            user_ip = user_public_ip,
            ec2_client = ec2_client,
            ec2_resource = ec2_resource
        )
        
        data["security_group_name"] = security_group_name
        data["vpc_id"] = vpc_id
    else:
        logger.info("Querying AWS for VPC ID of VPC \"%s\"" % vpc_name)
        resp = ec2_client.describe_vpcs(
            Filters = [{
                'Name': 'tag:Name',
                'Values': [
                    vpc_name  
                ],
            }],
        )
        
        if len(resp['Vpcs']) == 0:
            log_error("Could not find any VPCs with name \"%s\"" % vpc_name)
            exit(1)
        elif len(resp['Vpcs']) > 1:
            log_warning("Found multiple VPCs with name similar to \"%s\"" % vpc_name)
            log_warning("Please enter the number of the VPC you wish to use: ")
            
            counter = 1
            vpc_names = {}
            for vpc in resp['Vpcs']:
                name_of_vpc = None 
                for tag in resp['Vpcs']['Tags']:
                    if tag['Key'] == "Name":
                        name_of_vpc = tag['Value']
                        vpc_names[counter] = name_of_vpc
                        break 
                        
                if name_of_vpc == None:
                    log_error("Could not determine name of one of the VPCs returned by EC2Client::DescribeVPCs: %s" % str(vpc))
                
                print("%d - \"%s\" - %s" % (counter, resp['Vpcs'][0]['VpcId'], name_of_vpc))
                counter += 1
            
            # Ask the user to pick the VPC from all of the VPCs that were returned by the ec2_client.describe_vpcs() call.
            while True:
                selection_str = input(">").strip() 
                
                # Convert to an int.
                try:
                    selection_int = int(selection_str)
                except ValueError:
                    log_error("Please enter a numerical value. You entered \"%s\"" % selection_str)
                    continue
                
                # Make sure it's at least 1.
                if selection_int <= 0:
                    log_error("Please enter a positive numerical value between 1 and %d (inclusive). You entered \"%s\"" % (len(resp['Vpcs']), selection_str))
                    continue
                
                # Make sure it's not too large.
                if selection_int > len(resp['Vpcs']):
                    log_error("Please enter a numerical value between 1 and %d (inclusive). You entered \"%s\"" % (len(resp['Vpcs']), selection_str)) 
                    continue
                
                # Arrays in Python are zero-indexed.
                # But we numbered the choices between 1 and len(resp['Vpcs']).
                # So, subtract one from whatever the user specified. 
                selection_int = selection_int - 1
                
                selected_vpc = resp['Vpcs'][selection_int]
                selected_vpc_id = selected_vpc['VpcId']
                
                user_wants_to_continue = False 
                while True:
                    print() 
                    logger.info("You selected VPC \"%s\" with ID=%s. Is this correct? [y/n]" % (vpc_names[selection_int + 1], selected_vpc_id))
                    
                    correct = input(">").strip() 
                    
                    if correct.strip().lower() == "y" or correct.strip().lower() == "yes":
                        print() 
                        user_wants_to_continue = True 
                        vpc_id = selected_vpc_id
                        print()
                        break 
                    elif correct.strip().lower() == "n" or correct.strip().lower() == "no":
                        log_important("User elected not to continue. This script will now terminate.")
                        print()
                        user_wants_to_continue = False 
                        break 
                    else:
                        log_error("Please enter \"y\" for yes or \"n\" for no. You entered: \"%s\"" % correct)
                        continue
                
                if user_wants_to_continue:
                    log_important("Selected VPC \"%s\" with ID=%s." % (vpc_names[selection_int + 1], selected_vpc_id))
                    break 
                else:
                    log_important("Please enter the number of the VPC you wish to use: ")
                    continue 
        else:
            vpc_id = resp['Vpcs'][0]['VpcId']
        
        data["vpc_id"] = vpc_id
            
    log_success("Resolved VPC ID of VPC \"%s\" as %s" % (vpc_name, vpc_id))
    
    if not os.path.exists("./infrastructure_json"):
        os.mkdir("./infrastructure_json")
    
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
      
    if not skip_eks:
        create_eks_openwhisk_cluster(
            aws_profile_name = aws_profile_name, 
            aws_region = aws_region, 
            vpc_id = vpc_id,
            vpc_name = vpc_name,
            eks_cluster_name = eks_cluster_name,
            ec2_client = ec2_client,
            create_eks_iam_role = not skip_iam_role_creation,
            eks_iam_role_name = eks_iam_role_name,
        )
    
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    logger.info("Creating EC2 launch templates and instance groups now.")
    
    sec_grp_resp = ec2_client.describe_security_groups(
        Filters = [{
            'Name': 'vpc-id',
            'Values': [vpc_id]   
        }]
    )
    security_group_ids = []
    for security_group in sec_grp_resp['SecurityGroups']:
        security_group_id = security_group['GroupId']
        security_group_ids.append(security_group_id)
    
    data["security_group_ids"] = security_group_ids
    
    create_launch_templates_and_instance_groups(
        ec2_client = ec2_client,
        autoscaling_client = autoscaling_client,
        security_group_ids = security_group_ids,
        lfs_client_ags_it = lfs_client_ags_it,
        hopsfs_client_ags_it = hopsfs_client_ags_it,
        hopsfs_namenode_ags_it = hopsfs_namenode_ags_it,
        skip_launch_templates = skip_launch_templates,
        skip_autoscaling_groups = skip_autoscaling_groups,
        data = data
    )
    
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    # Get the subnet ID(s).
    resp_subnet_ids = ec2_client.describe_subnets(
        Filters = [{
            'Name': 'vpc-id',
            'Values': [vpc_id]   
        }]
    )
    subnet_ids = []
    public_subnet_ids = []
    private_subnet_ids = []
    for subnet in resp_subnet_ids['Subnets']:
        subnet_id = subnet['SubnetId']
        subnet_ids.append(subnet_id)
        
        identified_privacy_type = False 
        subnet_name = None 
        for tag in subnet['Tags']:
            if tag['Key'] == 'PrivacyType':
                privacy_type:str = tag['Value']
                
                if privacy_type.strip().lower() == "private":
                    private_subnet_ids.append(subnet_id)
                    identified_privacy_type = True
                    break 
                elif privacy_type.strip().lower() == "public":
                    public_subnet_ids.append(subnet_id)
                    identified_privacy_type = True 
                    break 
                else:
                    log_error("Unexpected value found for \"PrivacyType\" tag for subnet \"%s\": %s" % (subnet_id, privacy_type))
                    exit(1) 
            elif tag['Key'] == 'Name':
                subnet_name = tag['Value']
        
        # If they didn't have the 'PrivacyType' tag, then try to use their name.
        if not identified_privacy_type:
            if subnet_name is not None:
                if "private" in subnet_name.strip().lower():
                    private_subnet_ids.append(subnet_id)
                    identified_privacy_type = True
                elif "public" in subnet_name.strip().lower():
                    public_subnet_ids.append(subnet_id)
                    identified_privacy_type = True 
                else:
                    log_error("Could not identify privacy type of subnet %s (id=%s)" % (subnet_name, subnet_id))
                    exit(1)
            else:
                log_error("Could not identify name or privacy type of subnet %s" % subnet_id)
                exit(1)
    
    if len(subnet_ids) == 0:
        log_error("Could not find any subnets within VPC %s." % vpc_id)
        exit(1)
    
    if len(public_subnet_ids) == 0:
        log_error("Could not find any public subnet IDs.")
        log_error("Subnet IDs: %s" % str(subnet_ids))
        exit(1)

    if len(private_subnet_ids) == 0:
        log_error("Could not find any public subnet IDs.")
        log_error("Subnet IDs: %s" % str(subnet_ids))
        exit(1)
    
    data['subnet_ids'] = subnet_ids
    data['public_subnet_ids'] = public_subnet_ids
    data['private_subnet_ids'] = private_subnet_ids
    
    ###########################
    # Setup MySQL Cluster NDB #
    ###########################
    ndb_part(
        do_create_ndb_cluster = do_create_ndb_cluster,
        do_start_ndb_cluster = do_start_ndb_cluster,
        do_populate_mysql_ndb_tables = do_populate_mysql_ndb_tables,
        ndb_datanode_data_directory = ndb_datanode_data_directory,
        ndb_mgm_data_directory = ndb_mgm_data_directory,
        num_ndb_datanodes = num_ndb_datanodes,
        security_group_ids = security_group_ids,
        public_subnet_ids = public_subnet_ids,
        ndb_manager_instance_type = ndb_manager_instance_type,
        ndb_datanode_instance_type = ndb_datanode_instance_type,
        ndb_mgm_private_ipv4 = ndb_mgm_private_ipv4,
        ssh_keypair_name = ssh_keypair_name,
        ssh_key_path = ssh_key_path,
        ec2_resource = ec2_resource,
        ec2_client = ec2_client,  
        data = data,
        infrastructure_json = infrastructure_json,
        current_datetime = current_datetime
    )

    zk_node_public_IPs = None
    if create_zookeeper_vms:
        logger.info("Creating the λFS ZooKeeper nodes now.")
        zk_node_IDs = create_lambda_fs_zookeeper_vms(
            ec2_resource = ec2_resource, 
            ssh_keypair_name = ssh_keypair_name, 
            num_vms = num_lambda_fs_zk_vms, 
            security_group_ids = security_group_ids,
            subnet_id = public_subnet_ids[1],
            instance_type = lambdafs_zk_instance_type)
        log_success("Created %d ZooKeeper node(s): %s" % (len(zk_node_IDs), str(zk_node_IDs)))
        
        data["zk_node_IDs"] = zk_node_IDs
        
        logger.info("Sleeping for 30 seconds so that ZooKeeper VMs can start.")
        for _ in tqdm(range(121)):
            sleep(0.25)
        
        logger.info("Updating ZooKeeper configuration now.")
        zk_node_public_IPs = update_zookeeper_config(ec2_client = ec2_client, instance_ids = zk_node_IDs, ssh_key_path = ssh_key_path, zookeeper_jvm_heap_size = zookeeper_jvm_heap_size, data = data)
    
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    if do_start_zookeeper_cluster:
        logger.info("Starting ZooKeeper cluster now.")
        
        # These will be done if we didn't create the VMs during the execution of this script, in which case we expect the user to pass them in.
        if zk_node_public_IPs == None:
            zk_node_public_IPs = arguments.get("zk_node_public_IPs", [])
            
            # Log an error if we couldn't find any IPs.
            if len(zk_node_public_IPs) == 0:
                log_error("No ZooKeeper VM IPs. Cannot start ZooKeeper.")
        
        # Only start the cluster if the number of IPs is greater than 0.
        if len(zk_node_public_IPs) > 0:
            start_zookeeper_cluster(ips = zk_node_public_IPs, ssh_key_path = ssh_key_path)
            
            log_success("Successfully started the ZooKeeper cluster. Sleeping for a few seconds, then populating ZK cluster.")
            for _ in tqdm(range(21)):
                sleep(0.25)
        
    if do_populate_zookeeper_cluster:
        # These will be done if we didn't create the VMs AND didn't start the VMs during the execution of this script, in which case we expect the user to pass them in.
        if zk_node_public_IPs == None:
            zk_node_public_IPs = arguments.get("zk_node_public_IPs", [])
            
            # Log an error if we couldn't find any IPs.
            if len(zk_node_public_IPs) == 0:
                log_error("No ZooKeeper VM IPs. Cannot start ZooKeeper.")
        
        # Only populate the cluster if the number of IPs is greater than 0.
        if len(zk_node_public_IPs) > 0:
            logger.info("Populating ZooKeeper cluster now.")
            populate_zookeeper(ips = zk_node_public_IPs, ssh_key_path = ssh_key_path)
    
    if do_create_lambda_fs_client_vm:
        logger.info("Creating λFS client virtual machine.")
        lambda_fs_primary_client_vm_id = create_lambda_fs_client_vm(ec2_resource = ec2_resource, ssh_keypair_name = ssh_keypair_name, instance_type = lfs_client_vm_instance_type, subnet_id = public_subnet_ids[1], security_group_ids = security_group_ids)
        log_success("Created λFS client virtual machine: %s" % lambda_fs_primary_client_vm_id)
        
        data["lambda_fs_primary_client_vm_id"] = lambda_fs_primary_client_vm_id
        
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    if do_create_hops_fs_client_vm:
        logger.info("Creating HopsFS client virtual machine.")
        hops_fs_primary_client_vm_id = create_hops_fs_client_vm(ec2_resource = ec2_resource, ssh_keypair_name = ssh_keypair_name, instance_type = hopsfs_client_vm_instance_type, subnet_id = public_subnet_ids[1], security_group_ids = security_group_ids)
        log_success("Created HopsFS client virtual machine: %s" % hops_fs_primary_client_vm_id)
        
        data["hops_fs_primary_client_vm_id"] = hops_fs_primary_client_vm_id
    
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    end_time = time.time()
    print()
    print()
    print()
    log_important("Script has finished execution. Time elapsed: %.4f seconds." % (end_time - start_time))
    log_important("Newly created AWS infrastrucutre:")
    for k,v in data.items():
        logger.info("%s: %s" % (k, str(v)))
    
    with open("./infrastructure_json/infrastructure_ids_%s.json" % current_datetime, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()