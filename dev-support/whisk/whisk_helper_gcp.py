import argparse
import subprocess
import time

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

# Example:
#
# wsk -i action create /whisk.system/namenode1 /home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar --main org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode --web true --kind namenode:1
#

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num-functions",
        dest = "num_functions",
        type = int,
        default = 5,
        help = "The number of serverless functions to create. Default: 5")

    parser.add_argument("-p", "--prefix",
        dest = "prefix",
        type = str,
        default = "namenode",
        help = "The base name of the serverless functions. Default: \"namenode\". The created functions will be named <prefix>1, <prefix>2, etc.")

    parser.add_argument("-i", "--image",
        dest = "docker_image",
        type = str,
        default = "scusemua/java8action:latest",
        help = "The name of the docker image to use. Default: \"scusemua/java8action:latest\"")

    parser.add_argument("-c", "--concurrency", default = 1, type = int, dest = "concurrency",
                        help = "the maximum intra-container concurrent activation LIMIT for the action (default 1)")

    parser.add_argument("--create",
        action = "store_true",
        help = "Create new functions (rather than update existing functions. Only one of `create` and `update` should be true.")

    parser.add_argument("--memory", type = int, default = 256, help = "Memory limit in MB for the function. Default: 256MB")

    parser.add_argument("--update",
        action = "store_true",
        help = "Update existing functions (rather than create new functions. Only one of `create` and `update` should be true.")

    parser.add_argument("-m", "--main-class",
        dest = "main_class", type = str, default = "org.apache.hadoop.hdfs.serverless.OpenWhiskHandler",
        help = "The fully-qualified class name containing the OpenWhisk function handler.")

    parser.add_argument("-j", "--path", dest = "jar_path", type = str, default = "/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar",
        help = "Path to the JAR file containing the ServerlessNameNode code.")

    #parser.add_argument("-t", "--tag", dest = "tag", type = str, default = "latest", help = "Docker image tag. Defaults to 'latest'")
    parser.add_argument("-k", "--kind", dest = "kind", type = str, default = "generic-namenode:10", help = "Options are 'generic-namenode:10' and 'generic-namenode:5'")

    arguments = parser.parse_args()

    num_functions = arguments.num_functions
    prefix = arguments.prefix
    do_update = arguments.update
    do_create = arguments.create
    docker_image = arguments.docker_image
    main_class = arguments.main_class
    jar_path = arguments.jar_path
    concurrency = arguments.concurrency
    memory = arguments.memory
    #tag = parser.tag
    kind = arguments.kind

    # Only one of `do_create` and `do_update` should be true.
    if (do_create and do_update):
        logger.error("Exactly one of `create` and `update` must be true, NOT both.")
        exit(1)
    elif (not do_create and not do_update):
        logger.error("Exactly one of `create` and `update` must be true.")
        exit(1)

    logger.debug("Number of functions to create: %d" % num_functions)
    logger.debug("Function prefix: \"%s\"" % prefix)
    logger.debug("Docker image: \"%s\"" % docker_image)

    for i in range(num_functions):
        function_name = "%s%d" % (prefix, i)

        if do_create:
            logger.debug("Creating function with name \"%s\"" % function_name)
            #command = "wsk -i action create /whisk.system/%s %s --main %s --web true --docker %s" % (function_name, jar_path, main_class, docker_image)
            command = "wsk -i action create %s %s --main %s --web true --concurrency %d --kind %s --memory %d --param actionMemory %d" % (function_name, jar_path, main_class, concurrency, kind, memory, memory)
        else:
            logger.debug("Updating function with name \"%s\"" % function_name)
            #command = "wsk -i action update /whisk.system/%s %s --main %s --web true --docker %s" % (function_name, jar_path, main_class, docker_image)
            command = "wsk -i action update %s %s --main %s --web true --concurrency %d --kind %s --memory %d --param actionMemory %d" % (function_name, jar_path, main_class, concurrency, kind, memory, memory)

        split_command = command.split(" ")

        logger.debug("Executing command: " + str(split_command))

        subprocess.run(split_command)

        time.sleep(1)