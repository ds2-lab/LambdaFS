#!/bin/sh

# Pass the path to your SSH key as the first argument to this script.

echo "This script expects all ZooKeeper VMs to have a tag named 'Component' with the value 'ZooKeeperVM'."
echo "If your ZooKeeper VMs are not tagged this way, then this script will not work."

echo "Using SSH key located at \"$1\""

for ip in $(aws ec2 describe-instances --filters Name=tag:Component,Values=ZooKeeperVM --query 'Reservations[*].Instances[*].{Instance:PublicIpAddress}' --output text)
do
    echo "Starting ZooKeeper server on VM located at $ip"

    ssh -i $1 ubuntu@$ip 'sudo /opt/zookeeper/bin/zkServer.sh start'
done