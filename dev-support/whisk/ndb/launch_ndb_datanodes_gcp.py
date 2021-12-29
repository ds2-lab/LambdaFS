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
import googleapiclient.discovery
import random
import datetime
import numpy as np

def write_ndbd_section(file: io.IOBase, hostname: str, nodeid: int, data_dir = "/usr/local/mysql/data"):
    """
    Add an [ndbd] section to the config.ini file using the given values.

    Arguments:
    ----------
        file (io.IOBase, probably io.TextIOWrapper):
            The config.ini file that we're writing to.

        hostname (str):
            The private IP address of the DataNode in question.

        nodeid (int):
            The NodeId field to use.

        data_dir (str):
            Tells the DataNode where to persist its data on disk.
    """
    file.write("[ndbd]\n")
    file.write("HostName=%s" % hostname)
    file.write("NodeId=%d" % nodeid)
    file.write("DataDir=%s" % data_dir)
    file.write("\n")

def write_ndb_mgmd_section(file: io.IOBase, hostname: str, nodeid: int):
    """
    Add an [ndb_mgmd] section to the config.ini file using the given values.

    Arguments:
    ----------
        file (io.IOBase, probably io.TextIOWrapper):
            The config.ini file that we're writing to.

        hostname (str):
            The private IP address of the Manager Node.

        nodeid (int):
            The NodeId field to use.

        data_dir (str):
            Tells the DataNode where to persist its data on disk.
    """
    file.write("[ndb_mgmd]\n")
    file.write("HostName=%s" % hostname)
    file.write("NodeId=%d" % nodeid)
    file.write("\n")

def write_mysqld_section(file: io.IOBase, hostname: str):
    """
    Add a [mysqld] section to the config.ini file.

    Arguments:
    ----------
        file (io.IOBase, probably io.TextIOWrapper):
            The config.ini file that we're writing to.

        hostname (str):
            The private IP address of the mysql client node.
    """
    file.write("[mysqld]\n")
    file.write("HostName=%s" % hostname)
    file.write("\n")

def wait_for_operation(compute, project, zone, operation):
    """
    Wait for a given GCP operation to complete.

    Source: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/d8d4951911029b4776bd8d34ad5a8832fe21787b/compute/api/create_instance.py#L128
    """
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

def create_instance(compute, project_id: str, zone: str, instance_name: str, machine_image: str):
    """
    Create a new Compute Engine virtual machine.

    Arguments:
    ----------
        compute: The GCP API object for Compute Engine.

        project_id (str): Your Google Cloud project ID.

        zone (str): Compute Engine zone to deploy to.

        instance_name (str): Name to use for the instance that we're creating.

        machine_image (str): Name of the machine image to use for the instance.
    
    Returns:
    --------
        An operation as returned by the GCP API.
    """
    pass 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num-vms", type = int, dest = "num_vms", default = 1, help = "Number of VMs to create.")
    parser.add_argument("-i", "--image", type = str, default = "ndb-datanode", help = "Machine image to use for the DataNode VMs")
    parser.add_argument("-t", "--instance-type", type = str, dest = "instance_type", default = "e2-standard-16", help = "Google Compute Engine instance type to use.")
    parser.add_argument("-o", "--output", type = str, default = "/var/lib/mysql-cluster/config.ini", help = "Filepath for the config.ini file that gets generated after creating the VMs.")
    parser.add_argument("-mii", "--manager-internal-ip", type = str, dest = "manager_internal_ip", default = "10.150.0.18", help = "Internal IP address of the MySQL Cluster NDB Manager Node.")
    parser.add_argument("-c", "--client-ip", type = str, dest = "hopsfs_client_ip", default = "10.150.0.17", help = "Internal IP of the HopsFS client node.")
    parser.add_argument("-d", "--data-directory", type = str, dest = "data_directory", default = "/usr/local/mysql/data", help = "Data directory for the NDB DataNodes. This is where they persist their data on-disk.")

    parser.add_argument("-p", "--project-id", type = str, dest = "project_id", default = "serverlessmds", help = "Your Google Cloud project ID.")
    parser.add_argument("-z", "--zone", default = "us-east4-c", type = str, help = "Compute Engine zone to deploy to.")
    #parser.add_argument("-mei", "--manager-external-ip", type = str, dest = "manager_ip", default = "34.85.243.59", help = "Internal IP address of the MySQL Cluster NDB Manager Node.")

    args = parser.parse_args()

    num_vms = args.num_vms
    image = args.image
    instance_type = args.instance_type
    config_file_location = args.output
    manager_internal_ip = args.manager_internal_ip
    hopsfs_client_ip = args.hopsfs_client_ip
    data_directory = args.data_directory
    project_id = args.project_id 
    zone = args.zone 
    current_nodeid = 1

    # According to the official documentation, the total maximum number of nodes in an NDB cluster is 255. This number includes all SQL nodes 
    # (MySQL Servers), API nodes (applications accessing the cluster other than MySQL servers), data nodes, and management servers. Thus, we
    # can calculate how many API nodes to define by subtracting from 255 the number of data nodes + 2 (for the manager node and HopsFS client).
    number_api_nodes = 255 - (num_vms + 2) 

    if (num_vms >= 253):
        raise ValueError("Too many DataNodes requested (%d). An NDB cluster can only support a total of 255 nodes, and two of these nodes are reserved for the Manager Node and the HopsFS Client Node." % num_vms)

    compute = googleapiclient.discovery.build('compute', 'v1')

    print("Creating %d virtual machines:\n\tIMAGE NAME: '%s'\n\tINSTANCE TYPE: %s\n\tDATA DIRECTORY: \"%s\"" % (num_vms, image, instance_type, data_directory))
    print("There will be %d [api] nodes defined in the config.ini file." % number_api_nodes)

    operations = []
    for i in range(num_vms):
        operation = create_instance(compute, project_id, zone, "ndb-datanode-%d" % i, image)
        operations.append(operation)

    print("Created %d virtual machines. Next, creating config.ini file at path '%s'." % (num_vms, config_file_location))

    ndbd_default_file = open("./ndbd_default.txt", 'r')
    ndb_config_file = open(config_file_location)
    ndb_config_file.write("# Python-generated configuration file.\n")

    # Add the [ndbd default] section to the top of the config.ini file.
    ndb_config_file.write(ndbd_default_file.read())
    ndbd_default_file.close()

    # Add the [ndb_mgmd] section to the config.ini file.
    write_ndb_mgmd_section(ndb_config_file, manager_internal_ip, current_nodeid)
    current_nodeid += 1

    # Add the [mysqld] section for the HopsFS client to the config.ini file.
    write_mysqld_section(ndb_config_file, hopsfs_client_ip)

    for i in range(num_vms):
        write_ndbd_section(ndb_config_file, None, current_nodeid, data_dir = data_directory)
        current_nodeid += 1

    print("Finished adding [ndbd] sections to config.ini file. Adding %d [api] section(s) next." % number_api_nodes)

    for _ in range(number_api_nodes):
        ndb_config_file.write("[api]\n")
    
    ndb_config_file.close()