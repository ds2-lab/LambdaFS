import argparse
import base64
import requests

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--port", type = int, default = 50010)
    parser.add_argument("-m", "--main", type = str, default = "org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode")
    parser.add_argument("-j", "--jar-path", dest = "jar_path", type = str, default = "/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar")

    args = parser.parse_args()

    port = args.port
    main = args.main
    jar_path = args.jar_path

    with open(jar_path, 'rb') as jar_file:
        base64_encoded_jar_file = base64.b64encode(jar_file.read())
        r = requests.post("127.0.0.1:%d/init" % port, json = {
            "value": {
                "main": main,
                "code": base64_encoded_jar_file
            }})