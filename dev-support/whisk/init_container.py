import argparse
import base64
import requests

# NOT USED/FINISHED.
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--port", type = int, default = 50010)
    parser.add_argument("--host", type = str, default = "127.0.0.1")
    parser.add_argument("-m", "--main", type = str, default = "org.apache.hadoop.hdfs.serverless.OpenWhiskHandler")
    parser.add_argument("-j", "--jar-path", dest = "jar_path", type = str, default = "/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar")

    args = parser.parse_args()

    port = args.port
    host = args.host
    main = args.main
    jar_path = args.jar_path

    with open(jar_path, 'rb') as jar_file:
        base64_encoded_jar_file = base64.b64encode(jar_file.read()).decode('utf-8')
        endpoint = "http://%s:%d/init" % (host, port)
        print("Issuing POST request to '%s' now." % endpoint)
        r = requests.post(endpoint, json = {
            "value": {
                "main": main,
                "code": base64_encoded_jar_file
            }})