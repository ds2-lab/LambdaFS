import argparse
import io
import time
import os
import random
import datetime
import subprocess
from termcolor import colored
from pssh.clients import ParallelSSHClient

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type = str, default = "./public_ips_no_user.txt", help = "File containing IPs.")
parser.add_argument("--start", action = 'store_true', help = "Start the NNS.")
parser.add_argument("--stop", action = 'store_true', help = "Stop the NNS.")
parser.add_argument("-k", "--key-file", dest = "key_file", type = str, help = "Path to keyfile.")
parser.add_argument("-u", "--user", type = str, default = "ben", help = "Username for SSH.")

args = parser.parse_args()

start = args.start
stop = args.stop
ip_file_path = args.input
key_file = args.key_file
user = args.user

hosts = []
with open(ip_file_path, 'r') as ip_file:
    hosts = [x.strip() for x in ip_file.readlines()]
    print("Hosts: %s" % str(hosts))

client = ParallelSSHClient(hosts)

if start:
    print("Starting the NameNodes.")
    output = client.run_command("sudo /home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/bin/hdfs --daemon start namenode")
elif stop:
    print("Stopping the NameNodes.")
    output = client.run_command("sudo /home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/bin/hdfs --daemon stop namenode")
else:
    print("[WARNING] Neither '--start' nor '--stop' was specified. Doing nothing.")
    output = None

for i in range(len(output)):
    host_output = output[i]
    for line in host_output.stdout:
        print(line)