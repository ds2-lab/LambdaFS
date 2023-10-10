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

CORE_NODEGROUP_NAME = "core-nodes"
INVOKER_NODEGROUP_NAME = "invoker-nodes"

"""

This script is provided to assist with automatically scaling your AWS EKS cluster up and down.

This can also be performed using the AWS CLI or the Amazon Web Console. 

"""

def get_args()->argparse.Namespace:
    pass 

def main():
    args = get_args()
    
if __name__ == "__main__":
    main() 