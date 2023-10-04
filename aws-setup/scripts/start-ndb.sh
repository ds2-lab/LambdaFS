#!/bin/sh

# Pass the path to your SSH key as the first argument to this script.

echo "This script expects the NDB Manager Node VM to have a tag named 'Component' with the value 'NDB_MGM_VM'."
echo "Likewise, the NDB Data Nodes should have tag named 'Component' with the value 'NDB_DataNodeVM'."
echo "If your NDB VMs are not tagged exactly in this way, then this script will not work."

ndb_mgm_ip = $(aws ec2 describe-instances --filters Name=tag:Component,Values=NDB_MGM_VM --query 'Reservations[*].Instances[*].{Instance:PublicIpAddress}' --output text)

echo "Starting NDB Manager Node on VM located at $ip"
ssh -i $1 ubuntu@$ip 'sudo -S /usr/local/bin/ndb_mgmd --skip-config-cache -f /var/lib/mysql-cluster/config.ini'

for ip in $(aws ec2 describe-instances --filters Name=tag:Component,Values=NDB_DataNodeVM --query 'Reservations[*].Instances[*].{Instance:PublicIpAddress}' --output text)
do
    echo "Starting NDB DataNode on VM located at $ip"
    ssh -i $1 ubuntu@$ip 'sudo /usr/local/bin/ndbmtd'
done