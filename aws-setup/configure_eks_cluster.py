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

os.system("color")

# This script is to be used after executing the `create_aws_infrastructure.py` script to create the AWS EKS cluster.
# Once the cluster has become operational, this script can be used to automatically perform the additional required steps.
#
# REQUIREMENTS:
# - kubectl

AmazonEBSCSIDriverPolicyARN = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
AmazonEBSCSIDriverAddonName = "aws-ebs-csi-driver"

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

def get_args() -> argparse.Namespace:
    """
    Parse the commandline arguments.
    """
    parser = argparse.ArgumentParser()
    
    # YAML
    parser.add_argument("-y", "--yaml", type = str, default = "config_aws.yaml", help = "The path of a YAML configuration file.") #, which can be used in-place of command-line arguments. If nothing is passed for this, then command-line arguments will be used. If a YAML file is passed, then command-line arguments for properties that CAN be defined in YAML will be ignored (even if you did not define them in the YAML file).")
    return parser.parse_args()

def update_kubectl_local(aws_eks_cluster_name:str):
    subprocess.run(["aws", "eks", "update-kubeconfig", "--name", aws_eks_cluster_name])

def update_kubectl_remote():
    pass 

def install_amazon_ebs_csi_driver(
    aws_eks_cluster_name:str = None,
    aws_account_id:str = None,
    aws_region:str = None,
    ebs_csi_driver_iam_role_name:str = None,
    eks_client = None,
    iam_client = None,
):
    """
    References:
    - https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html
    """
    # First, create the IAM role and attach the AWS-provided AmazonEBSCSIDriverPolicy policy.
    create_and_attach_iam_role_for_ebs_csi_driver(aws_eks_cluster_name = aws_eks_cluster_name, aws_account_id = aws_account_id, aws_region = aws_region, eks_client = eks_client, iam_client = iam_client, ebs_csi_driver_iam_role_name = ebs_csi_driver_iam_role_name)
    
    # Second, create the Amazon EBS CSI Driver Addon.
    create_ebs_csi_driver_addon(aws_eks_cluster_name = aws_eks_cluster_name, aws_account_id = aws_account_id, ebs_csi_driver_iam_role_name = ebs_csi_driver_iam_role_name, eks_client = eks_client)
    
    print()
    print()
    print()
    log_important("This script will now try to annotate the Kubernetes service account with the EBS CSI Driver IAM role.")
    log_important("If this fails, you can simply do this part manually by executing a single command:")
    print()
    log_important("kubectl annotate serviceaccount ebs-csi-controller-sa -n kube-system ks.amazonaws.com/role-arn=arn:aws:iam::%s:role/%s" % (aws_account_id, ebs_csi_driver_iam_role_name))
    
    # Finally, annotate the Kubernetes service account.
    annotate_k8s_service_account(aws_account_id = aws_account_id, ebs_csi_driver_iam_role_name = ebs_csi_driver_iam_role_name)

def create_and_attach_iam_role_for_ebs_csi_driver(
    aws_eks_cluster_name:str = None,
    aws_account_id:str = None,
    aws_region:str = None,
    ebs_csi_driver_iam_role_name:str = None,
    eks_client = None,
    iam_client = None,
):
    """
    References: 
    - https://docs.aws.amazon.com/eks/latest/userguide/csi-iam-role.html
    """
    if aws_eks_cluster_name is None:
        log_error("Parameter aws_eks_cluster_name (str) cannot be None.")
        exit(1)

    if aws_account_id is None:
        log_error("Parameter aws_account_id (str) cannot be None.")
        exit(1)  
    
    if aws_region is None:
        log_error("Parameter aws_region (str) cannot be None.")
        exit(1)  
        
    if eks_client is None:
        log_error("Parameter eks_client cannot be None.")
        exit(1)  
    
    if iam_client is None:
        log_error("Parameter eks_client cannot be None.")
        exit(1)  
    
    cluster_description_response = eks_client.describe_cluster(name = aws_eks_cluster_name)
    
    try:
        oidc_issuer = cluster_description_response['cluster']['identity']['oidc']['issuer']
        logger.debug("Resolved OIDC issuer: %s" % oidc_issuer)
    except:
        log_error("Could not resolve OIDC issuer for AWS EKS cluster %s." % aws_eks_cluster_name)
        log_error("You should look into how to add an OIDC issuer to your AWS EKS cluster.")
        log_error("See the following for a step-by-step guide: https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html")
        exit(1)
    
    with open("./kubernetes_configs/aws-ebs-csi-driver-trust-policy_tmp.json", "r") as source_file:
        with open("./kubernetes_configs/aws-ebs-csi-driver-trust-policy.json", "w") as dest_file:
            for line in source_file.readlines():
                line = line.replace("{ACCOUNT_ID}", aws_account_id)
                line = line.replace("{AWS_REGION}", aws_region)
                line = line.replace("{OIDC_ISSUER}", oidc_issuer)
                dest_file.write(line)

    logger.info("Creating IAM role now.")
    
    with open("./kubernetes_configs/aws-ebs-csi-driver-trust-policy.json", "r") as assume_role_policy_document_file:
        assume_role_policy_document = json.load(assume_role_policy_document_file)
        
        logger.debug("Successfully loaded 'assume-role' policy document from \"./kubernetes_configs/aws-ebs-csi-driver-trust-policy.json\"")
    
    # Create the IAM role.
    try:
        iam_client.create_role(
            Path = "/",
            RoleName = ebs_csi_driver_iam_role_name,
            AssumeRolePolicyDocument = assume_role_policy_document,
        )
    except Exception as ex:
        log_error("Exception encountered while trying to create the IAM role for Amazon EBS CSI driver.")
        raise ex 

    # Attach the IAM role.
    try:
        iam_client.attach_role_policy(
            RoleName = ebs_csi_driver_iam_role_name,
            PolicyArn = AmazonEBSCSIDriverPolicyARN
        )
    except Exception as ex:
        log_error("Exception encountered while trying to attach a policy to the IAM role for Amazon EBS CSI driver.")
        log_error("Role name (not hard-coded): %s" % ebs_csi_driver_iam_role_name)
        log_error("Policy ARN (hard-coded): %s" % AmazonEBSCSIDriverPolicyARN)
        raise ex 

def create_ebs_csi_driver_addon(
    aws_eks_cluster_name:str = None,
    aws_account_id:str = None,
    ebs_csi_driver_iam_role_name:str = None,
    eks_client = None,
):
    if aws_eks_cluster_name is None:
        log_error("Parameter aws_eks_cluster_name (str) cannot be None.")
        exit(1)

    if aws_account_id is None:
        log_error("Parameter aws_account_id (str) cannot be None.")
        exit(1)  
    
    if ebs_csi_driver_iam_role_name is None:
        log_error("Parameter ebs_csi_driver_iam_role_name (str) cannot be None.")
        exit(1)
        
    if eks_client is None:
        log_error("Parameter eks_client cannot be None.")
        exit(1)
    
    serviceAccountRoleArn = "arn:aws:iam::%s:role/%s"
    
    try:
        eks_client.create_addon(
            clusterName = aws_eks_cluster_name,
            addonName = AmazonEBSCSIDriverAddonName,
            serviceAccountRoleArn = serviceAccountRoleArn % (aws_account_id, ebs_csi_driver_iam_role_name)
        )
    except Exception as ex:
        log_error("Exception encountered while trying to create the Amazon EBS CSI Driver add-on.")
        raise ex 

def annotate_k8s_service_account(
    aws_account_id:str = None,
    ebs_csi_driver_iam_role_name:str = None,
):
    if aws_account_id is None:
        log_error("Parameter aws_account_id (str) cannot be None.")
        exit(1)  
    
    if ebs_csi_driver_iam_role_name is None:
        log_error("Parameter ebs_csi_driver_iam_role_name (str) cannot be None.")
        exit(1)
    
    if os.name == 'nt':
        try:
            p = subprocess.Popen("annotate_k8s_sa.bat %s %s" % (aws_account_id, ebs_csi_driver_iam_role_name), cwd="./scripts/")
            stdout, stderr = p.communicate()
            logger.info(stdout.read())
            log_error("%s" % stderr.read())
        except Exception as ex:
            print()
            print()
            log_error("Exception while attempting to run the 'annotate_k8s_sa.sh' script located in aws-setup/scripts/annotate_k8s_sa.sh.")
            log_important("Please perform this last step manually by executing the following command: ")
            log_important("kubectl annotate serviceaccount ebs-csi-controller-sa -n kube-system ks.amazonaws.com/role-arn=arn:aws:iam::%s:role/%s" % (aws_account_id, ebs_csi_driver_iam_role_name))
            exit(1)
    else:
        try:
            logger.info("Executing shell command via subprocess module.")
            logger.info("Command: sh ./scripts/annotate_k8s_sa.sh %s %s" % (aws_account_id, ebs_csi_driver_iam_role_name))
            subprocess.call(['sh', './scripts/annotate_k8s_sa.sh', 'aws_account_id', 'ebs_csi_driver_iam_role_name'])
        except Exception as ex:
            log_error("Exception while attempting to run the 'annotate_k8s_sa.sh' script located in aws-setup/scripts/annotate_k8s_sa.sh.")
            log_important("Please perform this last step manually by executing the following command: ")
            log_important("kubectl annotate serviceaccount ebs-csi-controller-sa -n kube-system ks.amazonaws.com/role-arn=arn:aws:iam::%s:role/%s" % (aws_account_id, ebs_csi_driver_iam_role_name))
            exit(1)

def main():
    global NO_COLOR
    
    logger.info("This script will perform some additional configuration of your AWS EKS cluster.")
    logger.info("This script cannot be executed until the AWS EKS cluster has become operational.")
    logger.info("This can be determined based on whether or not the cluster has entered the 'ACTIVE' state.")

    command_line_args = get_args()
    
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
            aws_eks_cluster_name = arguments.get("aws_eks_cluster_name", None)
            lambda_fs_vm_public_ipv4 = arguments.get("lambda_fs_vm_public_ipv4", None)
            ssh_key_path = arguments.get("ssh_key_path", None)
            aws_account_id = arguments.get("aws_account_id", None)
            ebs_csi_driver_iam_role_name = arguments.get("ebs_csi_driver_iam_role_name", "AmazonEKS_EBS_CSI_DriverRole")
            
            if aws_eks_cluster_name is None:
                log_error("Please provide the name of your AWS Elastic Kubernetes Service (EKS) cluster via the \"aws_eks_cluster_name\" parameter.")
                exit(1)
            
            if lambda_fs_vm_public_ipv4 is None:
                log_error("Please provide the public IPv4 of the primary lfs client and experiment driver VM via the \"lambda_fs_vm_public_ipv4\" parameter.")
                exit(1)
            
            if ssh_key_path is None:
                log_error("Please provide the path to your private SSH key via the \"ssh_key_path\" parameter.")
                exit(1)
            
            if aws_account_id is None:
                log_error("Please provide your AWS account ID via the \"aws_account_id\" parameter.")
                exit(1)
                
            if ebs_csi_driver_iam_role_name is None:
                log_error("Please provide a name for the IAM role to be created for the Amazon EBS CSI driver via the \"ebs_csi_driver_iam_role_name\" parameter.")
                exit(1)
            
    logger.info("Updating kubeconfig (and therefore kubectl) to \"point\" to the \"%s\" AWS EKS cluster." % aws_eks_cluster_name)
    update_kubectl_local(aws_eks_cluster_name)
    logger.info("Done.")
    
    session:boto3.Session = None 
    if aws_profile_name is not None:
        logger.info("Attempting to create AWS Session using explicitly-specified credentials profile \"%s\" now..." % aws_profile_name)
        try:
            session = boto3.Session(profile_name = aws_profile_name)
            log_success("Successfully created boto3 Session using AWS profile \"%s\"" % aws_profile_name)
        except Exception as ex: 
            log_error("Exception encountered while trying to use AWS credentials profile \"%s\"." % aws_profile_name)
            raise ex 
        # ec2_client = session.client('ec2', region_name = aws_region)
        eks_client = session.client('eks', region_name = aws_region)
        iam_client = session.client('iam', region_name = aws_region)
    else:
        # ec2_client = boto3.client('ec2', region_name = aws_region)
        eks_client = boto3.client('eks', region_name = aws_region)
        iam_client = boto3.client('iam', region_name = aws_region)
    
    install_amazon_ebs_csi_driver(aws_eks_cluster_name = aws_eks_cluster_name, aws_account_id = aws_account_id, aws_region = aws_region, eks_client = eks_client, iam_client = iam_client, ebs_csi_driver_iam_role_name = ebs_csi_driver_iam_role_name)

if __name__ == "__main__":
    main()