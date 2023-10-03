#!/bin/bash

# This script is to be used after executing the `create_aws_infrastructure.py` script to create the AWS EKS cluster.
# Once the cluster has become operational, this script can be used to automatically perform the additional required steps.
#
# REQUIREMENTS:
# - kubectl