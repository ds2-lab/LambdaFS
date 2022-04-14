####################################################
# Launch the MySQL Cluster NDB DataNode VMs on GCP #
# Author: Benjamin Carver                          #
#                                                  #
# This script is used to automate the process of   #
# launching VMs to host DataNodes on GCP.          #
#
# It automatically launches the VMs, grabs their   #
# IP addresses, and generates a config.ini file    #
# for the NDB Manager Node.                        #
####################################################

import argparse
import io
import time
import os
import random
import datetime
import subprocess
from termcolor import colored

if os.name == 'nt':
    import colorama
    colorama.init()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num-vms", type = int, dest = "num_vms", default = 1, help = "Number of VMs to create.")
    parser.add_argument("-s", "--starting-index", type = int, dest = "starting_index", default = 0, help = "Index to start naming the instances at")
    parser.add_argument("-i", "--image", type = str, default = "vanilla-hopsfs-namenode-3", help = "Machine image to use for the DataNode VMs")
    parser.add_argument("-t", "--machine-type", type = str, dest = "machine_type", default = "c2-standard-16", help = "Google Compute Engine machine type to use.")
    parser.add_argument("-o", "--output", type = str, default = "/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/etc/hadoop", help = "Filepath for the config.ini file that gets generated after creating the VMs.")
    parser.add_argument("-k", "--key-file", type = str, help = "Key file for SSH onto newly-created VMs.")
    parser.add_argument("-p", "--project-id", type = str, dest = "project_id", default = "serverlessmds", help = "Your Google Cloud project ID.")
    parser.add_argument("-z", "--zone", default = "us-east4-c", type = str, help = "Compute Engine zone to deploy to.")

    args = parser.parse_args()

    num_vms = args.num_vms
    image = args.image
    machine_type = args.machine_type
    config_file_location = args.output
    project_id = args.project_id
    zone = args.zone
    key_file_path = args.key_file
    starting_index = args.starting_index
    current_nodeid = 1 + starting_index

    # According to the official documentation, the total maximum number of nodes in an NDB cluster is 255. This number includes all SQL nodes
    # (MySQL Servers), API nodes (applications accessing the cluster other than MySQL servers), data nodes, and management servers. Thus, we
    # can calculate how many API nodes to define by subtracting from 255 the number of data nodes + 2 (for the manager node and HopsFS client).
    number_api_nodes = 255 - (num_vms + 2)

    if (num_vms >= 253):
        raise ValueError("Too many DataNodes requested (%d). An NDB cluster can only support a total of 255 nodes, and two of these nodes are reserved for the Manager Node and the HopsFS Client Node." % num_vms)

    print("\nCreating " + colored(num_vms, 'green') + " virtual machines:\n\tIMAGE NAME: " + colored("'" + image + "'", 'green') + "\n\tMACHINE TYPE: " + colored("'" + machine_type + "'", 'green'))

    instance_names = []
    command = "gcloud beta compute instances create "
    for i in range(starting_index, num_vms + starting_index):
        instance_name = "vanilla-hops-namenode-%d" % i
        instance_names.append(instance_name)
        command += instance_name + " "

    command_end = "--project=%s --zone=%s --machine-type=%s --network-interface=network-tier=PREMIUM,subnet=default --maintenance-policy=MIGRATE --service-account=858581583747-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --min-cpu-platform=Automatic --tags=http-server,https-server --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any --source-machine-image=%s" % (project_id, zone, machine_type, image)

    command += command_end

    print("Executing command:\n" + colored(command, 'red'))

    response = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().decode().split()

    # The response is returned as an array of strings. The first seven are column headers.
    response_columns = response[0:7]

    # The remaining entries are the values in the columns.
    response_values = response[7:]
    private_ips = []
    public_ips = []

    for i in range(num_vms):
        # We're basically going row-by-row, except it's a 1D array.
        # We grab both the private and public IP addresses.
        private_ip_col_index = (6 * i) + 3
        private_ips.append(response_values[private_ip_col_index])

        public_ip_col_index = (6 * i) + 4
        public_ips.append(response_values[public_ip_col_index])

    print("Private IPv4s: " + str(private_ips))
    print("Public IPv4s: " + str(public_ips))

    # Write all of the private ips to a file so we can PSSH to start all the DataNodes.
    with open("./private_ips.txt", "w") as private_ips_file:
        for private_ip in private_ips:
            private_ips_file.write("ben@" + private_ip + "\n")

    # Also just write out all the public ips to a file without the username first, in case we need it for some reason.
    with open("./public_ips_no_user.txt", "w") as public_ips_file:
        for public_ip in public_ips:
            public_ips_file.write(public_ip + "\n")

    print("Created %d virtual machines." % (num_vms))

    # Sleep for a while to ensure the instances get launched.
    time.sleep(30)

    base_file_contents = \
    """
    <?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <!--
      Licensed under the Apache License, Version 2.0 (the "License");
      you may not use this file except in compliance with the License.
      You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

      Unless required by applicable law or agreed to in writing, software
      distributed under the License is distributed on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
      limitations under the License. See accompanying LICENSE file.
    -->

    <!-- Put site-specific property overrides in this file. -->

    <configuration>
            <property>
                    <name>fs.default.name</name>
                    <value>hdfs://%s:8020</value>
            </property>
            <property>
                    <name>hadoop.security.authentication</name>
                    <value>simple</value>
            </property>
            <property>
                    <name>hadoop.security.authorization</name>
                    <value>false</value>
            </property>
            <property>
                    <name>dfs.namenode.quota.enabled</name>
                    <value>false</value>
            </property>
    </configuration>
    """

    for i in range(0, len(public_ips)):
        public_ip = public_ips[i]
        private_ip = private_ips[i]
        file_contents = (base_file_contents % private_ip).encode('utf-8')

        ssh_cmd = ['ssh', '-i', key_file_path, '-o', 'StrictHostKeyChecking=no', 'ben@%s' % public_ip,
        "rm %s/core-site.xml; cat - > %s/core-site.xml" % (config_file_location, config_file_location)]

        print("Executing command:\n" + colored(ssh_cmd, 'red'))

        p = subprocess.Popen(ssh_cmd, stdin=subprocess.PIPE)

        p.stdin.write(file_contents)