# Serverless Hops Hadoop Distribution

**Hops** (<b>H</b>adoop <b>O</b>pen <b>P</b>latform-as-a-<b>S</b>ervice) is a next generation distribution of [Apache Hadoop](http://hadoop.apache.org/core/) with scalable, highly available, customizable metadata. Hops consists internally of two main sub projects, HopsFs and HopsYarn. <b>HopsFS</b> is a new implementation of the Hadoop Filesystem (HDFS), that supports multiple stateless NameNodes, where the metadata is stored in [MySQL Cluster](https://www.mysql.com/products/cluster/), an in-memory distributed database. HopsFS enables more scalable clusters than Apache HDFS (up to ten times larger clusters), and enables NameNode metadata to be both customized and analyzed, because it can now be easily accessed via a SQL API. <b>HopsYARN</b> introduces a distributed stateless Resource Manager, whose state is migrated to MySQL Cluster. This enables our YARN architecture to have no down-time, with failover of a ResourceManager happening in a few seconds. Together, HopsFS and HopsYARN enable Hadoop clusters to scale to larger volumes and higher throughput.

We have modified HopsFS to work with serverless functions. Currently we support the [OpenWhisk](https://openwhisk.apache.org/) serverless platform. Serverless functions enable better scalability and cost-effectiveness as well as ease-of-use.

# How to Build

### Software Required
For compiling the Hops Hadoop Distribution you will need the following software.
- Java 1.7 or higher
- Maven
- cmake for compiling the native code 
- [Google Protocol Buffer](https://github.com/google/protobuf) Version [2.5](https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz)
- [MySQL Cluster NDB](https://dev.mysql.com/downloads/cluster/) native client library 
- Kubernetes
- Helm
- Docker

We combine Apache and GPL licensed code, from Hops and MySQL Cluster, respectively, by providing a DAL API (similar to JDBC). We dynamically link our DAL implementation for MySQL Cluster with the Hops code. Both binaries are distributed separately.

Perform the following steps in the following order to compile the Hops Hadoop Distribution.

### Preparing Your VM Image

The following steps can be performed to create a virtual machine capable of building and driving Serverless HopsFS. These steps have been tested using a fresh virtual machine on both Google Cloud Platform (GCP) and IBM Cloud Platform (IBM Cloud). On GCP, the virtual machine was running Ubuntu 18.04.5 LTS (Bionic Beaver). On IBM Cloud, the virtual machine was running Ubuntu 18.04.6 LTS (Bionic Beaver).

#### Install JDK 1.8

Execute the following commands to install JDK 8. These are the commands we executed when developing Serverless HopsFS.

```
sudo apt-get purge openjdk*
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update 
sudo apt install openjdk-8-jre-headless
sudo apt-get install openjdk-8-jdk
```

See [this](https://askubuntu.com/questions/790671/oracle-java8-installer-no-installation-candidate) AskUbuntu thread for details on why these commands are used.

The exact versions of the JRE and JDK that we used are:
```
$ java -version
openjdk version "1.8.0_292"
OpenJDK Runtime Environment (build 1.8.0_292-8u292-b10-0ubuntu1~18.04-b10)
OpenJDK 64-Bit Server VM (build 25.292-b10, mixed mode)
```

Now you need to set your `JAVA_HOME` environment variable. You can do this for your current session via `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/` (though you should verify this is the directory that Java has been installed to). To make this permanent, add that command to the bottom of your `~/.bashrc` file.

#### Install Maven

```
sudo apt-get -y install maven
```

#### Install other required libraries

```
sudo apt-get -y install build-essential autoconf automake libtool cmake zlib1g-dev pkg-config libssl-dev libsasl2-dev
```

#### Install Protocol Buffers 2.5.0

```
cd /usr/local/src/
sudo wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
sudo tar xvf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0
sudo ./autogen.sh
sudo ./configure --prefix=/usr
sudo make
sudo make install
protoc --version
```

See [this](https://stackoverflow.com/questions/29797763/how-do-i-install-protobuf-2-5-on-arch-linux-for-compiling-hadoop-2-6-0-using-mav) StackOverflow post for details on why these commands are used. 

#### Install Bats 

Bats is used when building `hadoop-common-project`, more specifically the `hadoop-common` module. It can be installed with `sudo apt-get install bats`.

#### Additional Optional Libraries

The following libraries are all optional. We installed them when developing Serverless HopsFS.

- **Snappy Compression**: `sudo apt-get install snappy libsnappy-dev`

- **Bzip2**: `sudo apt-get install bzip2 libbz2-dev`

- **Jansson** (C library for JSON): `sudo apt-get install libjansson-dev`

- **Linux FUSE**: `sudo apt-get install fuse libfuse-dev`

- **ZStandard compression**: `sudo apt-get install zstd`

### Installing and Building the Serverless HopsFS Source Code

#### Database Abstraction Layer

**TODO: The hops-metadata-dal and hops-metadata-dal-impl-ndb layers are included in this base project. They should not be retrieved separately. (The custom libndbclient.so file can/should be retrieved separately, however.)**

```sh
git clone https://github.com/hopshadoop/hops-metadata-dal
```
The `master` branch contains all the newly developed features and bug fixes. For more stable version you can use the branches corresponding to releases. If you choose to use a release branch then also checkout the corresponding release branch in the other Hops Projects.

```sh
git checkout master
mvn clean install -DskipTests
```

#### Database Abstraction Layer Implementation

**TODO: The hops-metadata-dal and hops-metadata-dal-impl-ndb layers are included in this base project. They should not be retrieved separately. (The custom libndbclient.so file can/should be retrieved separately, however.)**

```sh
git clone https://github.com/hopshadoop/hops-metadata-dal-impl-ndb
git checkout master
mvn clean install -DskipTests
```
This project also contains c++ code that requires NDB `libndbclient.so` library. Download the [MySQL Cluster Distribution](https://dev.mysql.com/downloads/cluster/) and extract the `libndbclient.so` library. Alternatively you can download a custom MySQL Cluster library from our servers. Our custom library supports binding I/O threads to CPU cores for better performance.		
```sh
cd tmp
wget https://archiva.hops.works/repository/Hops/com/mysql/ndb/clusterj-native/7.6.10/clusterj-native-7.6.10-natives-linux.jar
unzip clusterj-native-7.6.10-natives-linux.jar
cp libndbclient.so /usr/lib
```

See this [section](#connecting-the-driver-to-the-database) for specifying the database `URI` and `username/password`. 

#### Building Hops Hadoop 
```sh
git clone https://github.com/hopshadoop/hops
git checkout master
```
##### Building a Distribution

First, enter the `hadoop-build-tools` directory and execute `mvn install`. Then go back to the root project directory.

Do this AFTER manually/explicitly building `hops-metadata-dal` and `hops-metadata-dal-impl-ndb` first.

```sh
mvn package generate-sources -Pdist,native -DskipTests -Dtar -Dmaven.test.skip=true
```

If you get `maven-enforcer-plugin` errors about dependency convergence and whatnot, you _may_ be able to get it to work by adding the `-Denforcer.fail=false` flag to the build command. Obviously this can cause issues if there are really some critical errors, but sometimes the dependency convergence issues can be ignored.

#### Connecting the Driver to the Database
There are two way to configure the NDB data access layer driver 
- **Hard Coding The Database Configuration Parameters: **
While compiling the database access layer all the required configuration parameters can be written to the ```./hops-metadata-dal-impl-ndb/src/main/resources/ndb-config.properties``` file. When the diver is loaded it will try to connect to the database specified in the configuration file. 

- **hdfs-site.xml: **
Add `dfs.storage.driver.configfile` parameter to hdfs-site.xml to read the configuration file from a sepcified path. For example, to read the configuration file in the current directory add the following the hdfs-site.xml
```xml
<property>
      <name>dfs.storage.driver.configfile</name>
      <value>hops-ndb-config.properties</value>
</property>  
```

### Common Errors/Issues During Building

- If at some point, you get an error that the `.pom` or `.jar` for `hadoop-maven-plugins` could not be found, go to the `/hadoop-maven-plugins` directory and execute `mvn clean install` to ensure that it gets built and is available in your local maven repository. 

- If you get dependency convergence errors from `maven-enforcer-plugin` about the `hops-metadata-dal` and `hops-metadata-dal-impl-ndb` projects, then you may be able to resolve them by building these two projects individually/one-at-a-time. Start with `hops-metadata-dal` and build it with `mvn clean install` (go to the root directory for that module and execute that command). Then do the same for `hops-metadata-dal-impl-ndb`. 

# Setting Up an OpenWhisk Kubernetes Deployment (via Helm)

We provide a separate repoistory in which we've pre-configured an OpenWhisk deployment specifically for use with Serverless HopsFS. This repository is available [here](https://github.com/Scusemua/openwhisk-deploy-kube).

Simply clone the repository and navigate to the `openwhisk-deploy-kube/helm/openwhisk` directory. Then execute the following command:

```
helm install owdev -f values.yaml .
```

This will install the OpenWhisk deployment on your Kubernetes cluster. Once everything is up-and-running, set the `apiHostName` property according to the Helm output. (When you execute the above command, Helm will tell you what to set the `apiHostName` property to.)

You can modify the deployment by changing the configuration parameters in the `openwhisk-deploy-kube/helm/openwhisk/values.yaml` file. After modifying the values, execute the following command (from the `openwhisk-deploy-kube/helm/openwhisk` directory):

```
helm upgrade owdev -f values.yaml .
```

# Building the NameNode Docker Image

We provide a separate repository containing a pre-configured OpenWhisk Docker image. This repository is available [here](https://github.com/Scusemua/openwhisk-runtime-java/tree/serverless-namenode).

Simply clone the repository. From the top-level directory, execute `./gradlew core:java8:distDocker` to build the image. 

# Export Control

This distribution includes cryptographic software. The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software. BEFORE using any encryption software, please check your country's laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted. See <http://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache Software Foundation distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software: Hadoop Core uses the SSL libraries from the Jetty project written by mortbay.org.

# Contact 

<ul>
<li><a href="https://community.hopsworks.ai/">Get support and report issues</a></li>
<li><a href="https://twitter.com/hopshadoop">Follow our Twitter account.</a></li>
</ul>

## YourKit

This project uses the YourKit Java profiler.

![YourKit](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with innovative and intelligent tools
for monitoring and profiling Java and .NET applications.
YourKit is the creator of <a href="https://secure-web.cisco.com/1W2R4YLY2hhGpl65mIlXRE6wo_Bgus3uAHj6kgi6Z1DflVl54XMtQ0_bG4gLyZxrKU1-JaPNp55Q2cQ3ftsKFpnUVDjPE4JGtM4ywlm950byJZCYI-1jgkH92zAIuQCFsDx005KZuETXhdtfIGZqSeQQ3ud1VvgxrlKrtMHbaUr37cdBphqhQeKaE8RxghLsfkxA36zftxus_N3L2xC5TDBkLd-MX8-52U5z1TQvszvleyF7LmsiQvsBqtLJi7VHFDke5NXhRgNcK-CEOltbSOapNonAY_rmdFaIBClzSk3rx561RISOnplyO8DJCbCJcb5VofGSz3w0bk2KkQpj82LYGGWx7gNJRSzVAsOvRb_lA7C1EoNYtlGgK9L061xx9T2HYE6BBDJgx7_QPEzpWqtEqXIQCknqKVaxcVewF06NYAYP8jgAlt977GYVwO0tAP8yR1Bvi0pcPFAYTu9fL8w/https%3A%2F%2Fwww.yourkit.com%2Fjava%2Fprofiler%2F">YourKit Java Profiler</a>,
<a href="https://secure-web.cisco.com/1piGOD1cy48OKLn1ALIiFbXeHFmGSfSNVVDqcuWnvPA2hUN3Z_Lv3HNh2ZhN_6Ji_c2oZo2sYQyBGroEsg--fCuHc-xSESSng1GpR9kkByOi0CLtj5i2NZFEEBEG6cTiultTSPxLP0Rambt3Y6CWrvS0EGIGkmYjrFXsuoT9HOxHok5bgMa8WkgtqBvECPAys02VWYQqTcu_JKRqsWIPIHIwRLhU6tlr-4DkpkLg5JSvVP0_FlyNCO0M0DFOiODn3T05GW3I28z8Jnlu3Ld6_-jawjTn-g3WY7B4br54NVXbNByKClKb57x8rqnPCxrvnpl5fc8piIvSjEArvu0gi26nZS2FBntop2HbLKgMpiBzFz5hFw3aK7Tn1DJC6fMtx1o3HGEM7QVM9wDTEblweqaB52HZUEZ4F1EvMJ7bUnzZzUMyUwN_pL_eAnglXrImic0HdcAIKNvGX1UVpG3XrWg/https%3A%2F%2Fwww.yourkit.com%2F.net%2Fprofiler%2F">YourKit .NET Profiler</a>,
and <a href="https://secure-web.cisco.com/1ujOsbmH037MKBTOxPNGDQ-UEwf1lyUZU_T8luSrnfjvlZ3dmoy52_vMceUkE0-aq7MEmqV-hQ6LyNLKOSfcz5R3vBy_2QsfcVzpzRDD7DQVlRseIAPvD2ChO9bAWRJXp37AFTiBdd1TrHwyexDCIkZp2tCk-AkILPmgMf_scfI0KEBw1qSsbM3ygYKQNgQCwDPIIiwyjs-534HqX7J-kGM3WB7tkWyhL8M9zPwATNoaGj4YjZ4h87bHqZM9rbPmUX-OFaf1LR8ORVTTICc6ZtF61D5RasbUElZHpCaJDPDH2QW1f609IchHvsL6XmB7hOsGmVZbWCZICM9saYYalQK5drI80YmB2e0vr_SiGUh5HW3YOxHJceECxIW9OoL7xEID4HzLW5GLpa0aEB6fWvfTE8rASBFbcAVD-Hij3fn8_7Eqnf4uBsO5qtU6OOdlTgKZQeClRKY3qdogYEB2oeQ/https%3A%2F%2Fwww.yourkit.com%2Fyoumonitor%2F">YourKit YouMonitor</a>.

# License

Hops is released under an [Apache 2.0 license](LICENSE.txt).

