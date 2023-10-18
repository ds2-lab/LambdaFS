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
        default = -1,
        help = "The number of serverless functions to create. Default: 5")

    parser.add_argument("-s", "--starting-index",
        dest = "starting_index",
        type = int,
        default = 0,
        help = "Index of first function to create or update. If --ending-index is specified, then we create NNs from [starting_index, ending_index). If n is specified, then we create n total functions, beginning with starting_index. If both are specified, we just use n.")

    parser.add_argument("-e", "--ending-index",
        dest = "ending_index",
        type = int,
        default = -1,
        help = "Index of last function to create or update. If --ending-index is specified, then we create NNs from [starting_index, ending_index). If n is specified, then we create n total functions, beginning with starting_index. If both are specified, we just use n.")

    parser.add_argument("-p", "--prefix",
        dest = "prefix",
        type = str,
        default = "namenode",
        help = "The base name of the serverless functions. Default: \"namenode\". The created functions will be named <prefix>1, <prefix>2, etc.")

    # We use a custom `namenode` "kind" for our OpenWhisk action, and OpenWhisk prohibits the specification of a docker image argument and a kind argument when creating or updating an action.
    # The docker image used by our custom `namenode` "kind" is specified in the runtimes.json file (same directory as the values.yaml) in the openwhisk-deploy-kube repository.
    # parser.add_argument("-i", "--image",
    #     dest = "docker_image",
    #     type = str,
    #     default = "scusemua/java8action:latest",
    #     help = "The name of the docker image to use. Default: \"scusemua/java8action:latest\"")

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
    # docker_image = arguments.docker_image
    main_class = arguments.main_class
    jar_path = arguments.jar_path
    concurrency = arguments.concurrency
    memory = arguments.memory
    #tag = parser.tag
    kind = arguments.kind
    starting_index = arguments.starting_index
    ending_index = arguments.ending_index

    # If -n was specified, we use that value to determine the number of functions we're creating,
    # even if --ending-index was also specified.
    if num_functions != -1:
        if ending_index != -1:
            logger.warn("Ignoring value of --ending-index (%d) because -n (%d) was also specified." % (ending_index, num_functions))

        ending_index = starting_index + num_functions
    # num_functions was -1. If ending_index is also -1, then error.
    elif ending_index == -1:
        logger.error("At least one of -n and --ending-index should be passed.")
        exit(1)

    num_functions_to_create = ending_index - starting_index

    if do_update:
        logger.debug("Updating %d functions, beginning with %s%d and ending with %s%d." % (num_functions_to_create, prefix, starting_index, prefix, ending_index - 1))
    elif do_create:
        logger.debug("Creating %d functions, beginning with %s%d and ending with %s%d." % (num_functions_to_create, prefix, starting_index, prefix, ending_index - 1))

    # Only one of `do_create` and `do_update` should be true.
    if (do_create and do_update):
        logger.error("Exactly one of `create` and `update` must be true, NOT both.")
        exit(1)
    elif (not do_create and not do_update):
        logger.error("Exactly one of `create` and `update` must be true.")
        exit(1)

    if (starting_index > ending_index):
        logger.error("The starting index (%d) cannot be greater than the ending index (%d)." % (starting_index, ending_index))
        exit(1)

    logger.debug("Number of functions to create: %d" % num_functions_to_create)
    logger.debug("Function prefix: \"%s\"" % prefix)

    for i in range(starting_index, ending_index):
        function_name = "%s%d" % (prefix, i)

        if do_create:
            logger.debug("Creating function with name \"%s\"" % function_name)
            command = "wsk -i action create %s %s --main %s --web true --concurrency %d --kind %s --memory %d --param actionMemory %d" % (function_name, jar_path, main_class, concurrency, kind, memory, memory)
        else:
            logger.debug("Updating function with name \"%s\"" % function_name)
            command = "wsk -i action update %s %s --main %s --web true --concurrency %d --kind %s --memory %d --param actionMemory %d" % (function_name, jar_path, main_class, concurrency, kind, memory, memory)

        split_command = command.split(" ")

        logger.debug("Executing command: " + str(split_command))

        subprocess.run(split_command)

        time.sleep(1)