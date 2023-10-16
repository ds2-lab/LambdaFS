# AWS Setup 

The following document provides a set of directions for creating the required AWS infrastructure to deploy and run λFS and HopsFS. Much of this process can be automated using the `create_aws_infrastructure.py` and `configure_eks_cluster.py` scripts. The `configure_eks_cluster.py` script should be executed once the AWS EKS cluster created by the `create_aws_infrastructure.py` script becomes operational. 

Please note that the `setup_tldr.md` document provides a shortened step-by-step version of these instructions. We recommend following those instructions by default and referring to this if you encounter issues or want to perform certain steps manually (instead of using the `create_aws_infrastructure.py` script).

# Requirements

This section outlines requirements and prerequisites for following these instructions. 

## Software

We tested these scripts on Windows 10 version 22H2 (OS Build 19045.3448) with Python 3.9.4 (tags/v3.9.4:1f2e308, Apr  6 2021, 13:40:21).

The `aws-setup/requirements.txt` includes a list of all required Python modules as used by the `create_aws_infrastructure.py` script. The version numbers explicitly listed in the `requirements.txt` file are the versions we had installed when creating and using the script. If you have more recent versions of any of the modules and the script does not work, then please try downgrading to the version numbers explicitly listed in the `requirements.txt` file. 

You must install and configure the `AWS CLI`. Please refer to the [general AWS CLI documentation](https://aws.amazon.com/cli/), [installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html), and [credentials configuration instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for this step.

You must install and configure `kubectl`, the Kubernetes command-line tool, which allows you to run commands against Kubernetes clusters. Please refer to the [official Kubernetes documentation](https://kubernetes.io/docs/tasks/tools/) for this step.

## Required Manual Configuration

The default values for most parameters will be sufficient. There are a few that must be specified explicitly. These include:
- `ssh_keypair_name`: In order to be able to connect to the various virtual machines used by λFS and HopsFS, you will need to create and register an *EC2 key pair* with AWS. (If you have already done so, then you may reuse an existing key pair, provided the key is an RSA key.) [See this documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) from AWS concerning "Amazon EC2 key pairs and Linux instances" for additional details. 
- `ssh_key_path`: This is a path to the private key of the EC2 key pair specified in the `ssh_keypair_name` parameter. **This must be an RSA key.** The key should be on the computer from which you run the `create_aws_infrastructure.py` script. **The key will also need to be copied to the $\lambda$FS and HopsFS primary client (experiment driver) virtual machines.** This step is performed automatically by the `create_aws_infrastructure` script. It may be worthwhile creating a new key to use with HopsFS if you do not want an existing key copied to these VMs.
- `aws_profile` (*possibly*): Depending on how you've configured the AWS credentials on your computer, you may be able to simply use the default AWS credentials profile. If you've not configured any AWS credentials on your computer, then this can be done by installing the [AWS CLI](https://aws.amazon.com/cli/). See [this AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) concerning "configuration and credential file settings" for more details.

Copying the `sample_config_aws.yaml` file to a `config_aws.yaml` file and using the default values for all other configurations parameters (aside from the parameters explicitly listed above) should be sufficient for creating all of the necessary components.

The `create_aws_infrastructure.py` script will automatically provision the following components:

- AWS Virtual Private Cloud (VPC)
- Security group
- Public & private subnets 
- EC2 virtual machines for:
  - MySQL NDB Cluster (used by both HopsFS and λFS)
    - The manager node and however many data nodes
  - "Primary" client VMs (which serve as experiment drivers) for HopsFS and λFS
  - ZooKeeper nodes used by λFS
- Launch templates for λFS clients, HopsFS clients, and HopsFS NameNodes. 
- Auto-scaling groups for λFS clients, HopsFS clients, and HopsFS NameNodes.
- AWS Elastic Kubernetes Service (EKS) cluster, onto which we deploy OpenWhisk, the FaaS platform used by λFS.
  - The script also creates a number of other componets required by the AWS EKS cluster, including an IAM role, service accounts, etc.

The script can be configured by creating a `config_aws.yaml` file within the same directory as the script (i.e., the `aws-setup/` directory). There is a `sample_config_aws.yaml` provided with explanations of the various configuration parameters. 

If you are interested in manually deploying or configuring your VPC, the `Networking` section of the `AWS Elastic Kubernetes Service (EKS)` instructions provides information about what is required in the VPC.

# AWS Elastic Kubernetes Service (EKS)

The AWS EKS cluster used by λFS can be created automatically using both the `create_aws_infrastructure.py` and `configure_eks_cluster.py` scripts. As described above, the `configure_eks_cluster.py` script should be executed once the AWS EKS cluster created by the `create_aws_infrastructure.py` script becomes operational. 

Nevertheless, we have found it to be a little tricky to deploy OpenWhisk on AWS EKS. The following are some useful resources concerning the creation of an AWS EKS cluster and the subsequent deployment of OpenWhisk onto the AWS EKS cluster:

- [Official OpenWhisk Documentation: "Deploying OpenWhisk on Amazon EKS"](https://github.com/apache/openwhisk-deploy-kube/blob/master/docs/k8s-aws.md). 
  - In particular, you should follow the ["Configuring OpenWhisk using SSL and IAM"](https://github.com/apache/openwhisk-deploy-kube/blob/master/docs/k8s-aws.md#configuring-openwhisk-using-ssl-and-iam) section. We also use the configuration described in the ["Configuring Openwhisk using SSL and Elastic Loadbalancers"](https://github.com/apache/openwhisk-deploy-kube/blob/master/docs/k8s-aws.md#configuring-openwhisk-using-ssl-and-iam) section; however, you can still use self-signed certificates if you are simply looking to test and experiment with λFS (which is *not* what their documentation says).
- [Amazon EBS CSI driver](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html). You will need to install the Amazon EBS CSI driver on your Amazon EKS cluster in order for OpenWhisk to work. The deployment script (`create_aws_infrastructure.py`) *should* perform this step for you automatically; however, if the script encounters any issues, then you can simply follow the AWS documentation to manually install the Amazin EBS CSI driver. We will also include our own instructions below, as supplementary material. 

## Manual Creation/Deployment Instructions

### **General Configuration**

You can manually create and deploy the AWS EKS cluster using either the AWS Web Console or the AWS Command Line Interface (CLI). The following instructions will be agnostic to the method you elect to use; the instructions will simply specify the required configuration.

You may name the AWS EKS cluster whatever you'd like. We will use `"LambdaFS_EKS_Cluster"`.

The `Kubernetes version` should be specified as 1.24, as this is what λFS was developed on. Other versions of Kubernetes may work fine, but we've not tested them with λFS ourselves. 

You must create an IAM role to serve as the "Amazon EKS cluster role". Please follow the [instructions in the official AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/service_IAM_role.html#create-service-role) concerning this step. We will refer to this role by name as `eksClusterRole`. 

### **Networking (VPC)**

Please specify the VPC created using the `create_aws_infrastructure.py` script. If you create the VPC yourself, then you must create the following components within that VPC:
- Security group (which we will refer to by name as `lambda-fs-security-group`).
  - The security group should have a rule of type `All traffic` (`All` protocols, `All` port range) whose `Source` is the security group itself.
  - The seceurity group should have *another* rule of type `SSH` (`TCP` protocol, port range `22`) whose `Source` is the IP address from which you'd like to SSH into VMs residing within the VPC. 
- Internet gateway
- Two "public" subnets with routes to the internet gateway. The `destination` of the routes should be `0.0.0.0/0`, and the `target` is the ID of the internet gateway. 
- NAT gateway (deployed in one of the public subnets)
- Two "private" subnets, both of which have a route to the NAT gateway. The `destination` of the routes should be `0.0.0.0/0`, and the `target` is the ID of the NAT gateway. 
- We set the `IPv4` of the VPC to `10.0.0.0/16`. 

For the `subnets` of your AWS EKS cluster, you should specify all of the subnets in your VPC. If you created your VPC using the `create_aws_infrastructure.py` script or manually created the VPC using the same configuration (i.e., the configuration specified in the bulleted list above), then select the two private subnets and the two public subnets. 

You should select the security group created during the VPC creation for the AWS EKS cluster.

Select `IPv4` for the `cluster IP address family`.

For `Cluster endpoint access`, select `Public`. 

There is no logging that must be configured. If you are performing this step using the AWS web console, then this means that you can skip the `Configure logging` step. For cluster add-ons, we are using `CoreDNS`, `kube-proxy`, and `Amazon VPC CNI`. These should be selected by-default when using the AWS web console to create the EKS cluster. We use the default settings (in the web console, at least) for these add-ons. 

After creating your AWS EKS cluster, it may take 10 - 15 minutes for the cluster to become operational. Once the cluster becomes operational, there are some additional steps that must be taken before deploying OpenWhisk. 

### **Enabling IAM principal access to your cluster**

Please refer to [this documentation](https://docs.aws.amazon.com/eks/latest/userguide/add-user-role.html) for this step.

### **Amazon EBS CSI Driver**

#### **Overview**

The Amazon Elastic Block Store (Amazon EBS) Container Storage Interface (CSI) driver manages the lifecycle of Amazon EBS volumes as storage for the Kubernetes Volumes that you create. This component is required in order for OpenWhisk to be deployed successfully.

AWS provides its own documentation for installing the Amazon EBS CSI driver onto an AWS EKS cluster. This documentation can be found [here](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html). In our experience, the documentation sufficiently covers the installation process. We highly recommend deploying a simple, sample application and verify that the CSI driver is working *before* attempting to deploy OpenWhisk onto your AWS EKS cluster. AWS provides such a sample documentation [here](https://docs.aws.amazon.com/eks/latest/userguide/ebs-sample-app.html) (along with instructions on how to setup and teardown the sample app).

#### **Installation Tips & Hints**

**Creating the Amazon EBS CSI driver IAM role**

The Amazon EBS CSI plugin requires IAM permissions to make calls to AWS APIs on your behalf. In order to perform this part of the installation process, you must have an existing AWS EKS cluster and an existing AWS Identity and Access Management (IAM) OpenID Connect (OIDC) provider for your cluster. If you are unsure as to whether or not you have one -- or if you know that you need to create one -- please see the following resource: ["creating an IAM OIDC provider for your cluster"](https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html).

**IMPORTANT:** Once you've installed the Amazon EBS CSI driver, there is an additional step that you should perform. Specifically, you should annotate the associated Kubernetes service account with the ARN of the EKS cluster IAM role. (This role was either created automatically via the `create_aws_infrastructure.py` script, or you created it while creating the AWS EKS cluster.)

To annotate the service account, please execute the following command, making sure to replace `111122223333` with your [account ID](https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-identifiers.html) and `AmazonEKS_EBS_CSI_DriverRole` with the name of the IAM role:

```sh
kubectl annotate serviceaccount ebs-csi-controller-sa \
    -n kube-system \
    eks.amazonaws.com/role-arn=arn:aws:iam::111122223333:role/AmazonEKS_EBS_CSI_DriverRole
```

Also, if you find you are still having errors, then you may need to modify the `EKS Cluster IAM role` such that it has the `sts:AssumeRoleWithWebIdentity` action specified in its trust policy/trust relationship. However, this itself may cause issues/errors (apparently?), so you should only add the `sts:AssumeRoleWithWebIdentity` action if you have reason to think you need to. (For example, you attempt to deploy the sample app used to determine if the EBS CSI driver is working. Upon describing the Kubernetes pod created by the app, you see errors stating that it is not authorized to perform some action and explicitly mentions `sts:AssumeRoleWithWebIdentity`.)

### **OpenWhisk NGINX Secret** 

You may encounter an error when deploying OpenWhisk (which we've not yet gone over in these instructions) concerning OpenWhisk's `gencerts` pod. This pod attempts to either use an existing "secret" called `<openwhisk-deployment-name>-nginx` or create a new one if no secret with that name exists. The creation process can sometimes fail with an error like the following (`testdev` is the name given to the OpenWhisk deployment):

```sh
Error from server (NotFound): secrets "testdev-nginx" not found
generating new testdev-nginx secret
generating server certificate request
Can't load /root/.rnd into RNG
139693137723840:error:2406F079:random number generator:RAND_load_file:Cannot open file:../crypto/rand/randfile.c:88:Filename=/root/.rnd
problems making Certificate Request
139693137723840:error:0D07A097:asn1 encoding routines:ASN1_mbstring_ncopy:string too long:../crypto/asn1/a_mbstr.c:107:maxsize=64
```

To avoid this, you can simply pre-create the `<openwhisk-deployment-name>-nginx` secret *before* attempting to deploy OpenWhisk. This process is performed automatically by the `configure_eks_cluster.py` script; however, if you are setting up the AWS EKS cluster manually, then you will (likely) need to perform this step yourself. If you elect to skip this step and deploy OpenWhisk without pre-creating the secret, and you ultimately encounter the error, then simply perform `helm uninstall <deployment-name>` to uninstall OpenWhisk from your Kubernetes cluster. Then, follow the steps here before trying again to deploy OpenWhisk. 

#### **Creating NGINX Secret**

Generate the certificate and key (replace `KEY` and `CERT` with whatever you want the generated files to be named):

```sh
openssl req -x509 -newkey rsa:4096 -keyout KEY.pem -out CERT.pem -sha256 -days 365 -nodes
```

Next, create the `secret`. The secret must be named `"<OpenWhisk-deployment-name>-nginx"`. The name of the OpenWhisk deployment is whatever you specify to `helm` when deploying the OpenWhisk chart via `helm install <deployment_name> values.yaml .` (as described later in the documentation). Once again, replace `KEY` and `CERT` with whatever you specified when generating the files:

```sh
kubectl create secret tls OPENWHISK_DEPLOYMENT_NAME-nginx --cert=CERT.pem --key=KEY.pem
```

If you named your OpenWhisk deployment `owdev`, then the command would be:

```sh
kubectl create secret tls owdev-nginx --cert=CERT.pem --key=KEY.pem
```

### **Labeling EKS Nodes as OpenWhisk Invokers**

OpenWhisk's `Invoker` component can only be scheduled onto Kubernetes nodes labeled with the key-value tag/pair `openwhisk-role=invoker`. 

Execute `kubectl label nodes --all openwhisk-role=invoker` to designate all nodes as invokers. Alternatively, you specify specific nodes by their ID in place of using the `--all` flag.

## Common Issues & Errors

In this section, we describe some commonly-encountered problems (and their solutions) when creating the AWS EKS cluster and deploying OpenWhisk onto it. 

### **ERROR: "Your current IAM principal doesn’t have access to Kubernetes objects on this cluster."**

You may see this error message displayed at the top of the AWS Web Console when viewing your AWS EKS cluster. In order to resolve this error, please follow the instructions outlined in the [Amazon EKS "View Kubernetes resources" documentation](https://docs.aws.amazon.com/eks/latest/userguide/view-kubernetes-resources.html#view-kubernetes-resources-permissions). 

This documentation mentions ensuring that the proper permissions are assigned "to the IAM principal that you're using." This should be the IAM Role that was in-use wherever you created the AWS EKS cluster from. For example, if you created the EKS cluster using scripts or the AWS CLI, then its the role associated with your credentials as configured on that machine. If you used the AWS Web Console, then its the IAM Role assumed by whatever IAM User is associated with the AWS Web Console. 

We have also found success by updating the `configmap` of the Kubernetes cluster as follows:
```sh
kubectl edit configmap aws-auth -n kube-system
```

Under the `mapRoles` field, you may have a `mapUsers` field at the same indentation level. If not, add one as follows:

```yaml
  mapUsers: |
    - groups:
      - system:masters
      userarn: arn:aws:iam::111222333444:user/<IAM principal user name>
    - groups:
      - system:masters
      userarn: arn:aws:iam::111222333444:root
```

Altogether, the `configmap` will look something like this:

```yaml
apiVersion: v1
data:
  mapRoles: |
    - groups:
      - system:bootstrappers
      - system:nodes
      rolearn: arn:aws:iam::111222333444:role/<EKS cluster IAM role>
      username: system:node:{{EC2PrivateDNSName}}
    - groups:
      - system:bootstrappers
      - system:nodes
      rolearn: arn:aws:iam::111222333444:role/<EKS node role>
      username: system:node:{{EC2PrivateDNSName}}
    - groups:
      - <eks-console-dashboard-full-access-group>
      rolearn: arn:aws:iam::111222333444:role/<IAM principal role>
  mapUsers: |
    - groups:
      - system:masters
      userarn: arn:aws:iam::111222333444:user/<IAM principal user name>
    - groups:
      - system:masters
      userarn: arn:aws:iam::111222333444:root
...
```

For additional information concerning this issue and how to fix it, please refer to [this AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/add-user-role.html).

### **Failure to Generate NGINX Secret When Deploying OpenWhisk**

You may encounter an error when deploying OpenWhisk (which we've not yet gone over in these instructions) concerning OpenWhisk's `gencerts` pod. This pod attempts to either use an existing "secret" called `<openwhisk-deployment-name>-nginx` or create a new one if no secret with that name exists. The creation process can sometimes fail with an error like the following (`testdev` is the name given to the OpenWhisk deployment):

```sh
Error from server (NotFound): secrets "testdev-nginx" not found
generating new testdev-nginx secret
generating server certificate request
Can't load /root/.rnd into RNG
139693137723840:error:2406F079:random number generator:RAND_load_file:Cannot open file:../crypto/rand/randfile.c:88:Filename=/root/.rnd
problems making Certificate Request
139693137723840:error:0D07A097:asn1 encoding routines:ASN1_mbstring_ncopy:string too long:../crypto/asn1/a_mbstr.c:107:maxsize=64
```

The steps to avoid/resolve this error are described above in the "**OpenWhisk NGINX Secret**" section.

# OpenWhisk

Once you've created and configured your AWS EKS Cluster, you can deploy OpenWhisk.

To do this, we recommend creating the λFS "primary" client and experimental driver virtual machine using the `create_aws_infrastructure.py` script. Alternatively, you can create and deploy an EC2 VM yourself using the AMI `ami-079ac4c46055b2bdf`. Ensure this virtual machine is running before connecting to the instance via SSH. 

You'll need to use the AWS CLI for this part. You should begin by configuring your AWS CLI, which ultimately amounts to providing your AWS account credentials:
``` sh
aws configure
```

Once you've done this, you should execute the following command, replacing `"lambda-fs-eks-cluster"` with the name of your AWS EKS cluster:
``` sh
aws eks update-kubeconfig --name lambda-fs-eks-cluster
```

This command updates your `kubectl` configuration so that it is "pointing to" the correct Kubernetes cluster.

Navigate to the `/home/ubuntu/repos/openwhisk-deploy-kube` directory. This directory contains a local GitHub repository of the repository found [here](https://github.com/Scusemua/openwhisk-deploy-kube). The repository contains a pre-configured deployment of OpenWhisk with the same settings as the one used by λFS.  

**NOTE:** Before deploying OpenWhisk, we recommend pre-creating the required NGINX `secret`. This process is described in the **Creating NGINX Secret** section above. If you executed the `configure_eks_cluster.py` script, then this step was already performed for you.

## Deploy Self-Signed Certificates

First, generate the self-signed certificates. You can change the `key.pem` and `cert.pem` filenames if desired for whatever reason. 

```sh
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes
```

Next, upload to IAM (changing the `cert.pem` and `key.pem` if you changed them in the previous command):

```sh
aws iam upload-server-certificate --server-certificate-name ow-self-signed-test --certificate-body file://cert.pem --private-key file://key.pem
```

Verify that the upload was successful by using the command:

```sh
aws iam list-server-certificates
```

Add the following to your `values.yaml`. Make sure to replace the `111222333444` with your AWS account ID. Likewise, use your certificate's ARN instead of the example one:

```yaml
whisk:
  ingress:
    ...
    type: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-internal: 0.0.0.0/0
      service.beta.kubernetes.io/aws-load-balancer-ssl-cert: arn:aws:iam::111222333444:server-certificate/ow-self-signed
    ...
```

## Deploying & Updating OpenWhisk

**IMPORTANT:** Please ensure you are using the `aws` branch of the `openwhisk-deploy-kube` repository.

To deploy OpenWhisk for the first time, navigate to the `openwhisk-deploy-kube/helm/openwhisk` directory and execute the following command. Note that you can change `owdev` to be whatever you want. It will be the name of the OpenWhisk Kubernetes deployment.

```sh
helm install owdev -f values.yaml .
```

If you wish to update your existing OpenWhisk deployment after modifying some settings, navigate to the `openwhisk-deploy-kube/helm/openwhisk` directory and execute the following command. Make sure to change `owdev` to whatever name you assigned to your OpenWhisk Kubernetes deployment.

``` sh
helm upgrade owdev -f values.yaml .
```

Shortly after you deploy your helm chart, an ELB should be automatically created. You can determine its hostname by issuing the command `kubectl get services -o wide`. 

Use the value in the the `EXTERNAL-IP` column for the nginx service and port 80 to define your wsk `apihost` property:

``` sh
wsk -i property set --apihost http://<EXTERNAL-IP>:80
```

**IMPORTANT:** Make sure to prepend `<EXTERNAL-IP>` with `"http://"` when setting the `apihost` property as is shown in the command above. 

You may also modify the `apiHostName` and `apiHostPort` fields of the `values.yaml` file to contain the new API host and port as specified in the command above.

``` yaml
whisk:
  # Ingress defines how to access OpenWhisk from outside the Kubernetes cluster.
  # Only a subset of the values are actually used on any specific type of cluster.
  # See the "Configuring OpenWhisk section" of the docs/k8s-*.md that matches
  # your cluster type for details on what values to provide and how to get them.
  ingress:
    apiHostName: http://<EXTERNAL-IP>
    apiHostPort: 80
    apiHostProto: "https"
...
```

You may monitor the progress of the OpenWhisk deployment by inspecting the various pods.

``` sh
kubectl get pods 
```

Once OpenWhisk is up-and-running, you are almost done setting everything up! The next step is to register the serverless NameNode deployments with OpenWhisk.

### **Creating the Serverless NameNode Deployments**

You must register a number of unique serverless NameNode deployments with OpenWhisk. There is a script that helps to automate this process available in at `whisk_helper_gcp.py`. Although this script is labeled "gcp", it will work for an AWS-based deployment of OpenWhisk just as well. In our evaluation of λFS, we used 20 NameNode deployments. In order to create 20 NameNode deployments using the provided script, execute the following command:

``` sh
python3 /home/ubuntu/repos/hops/dev-support/whisk/whisk_helper_gcp.py -n 20 --create --memory 20000 --concurrency 4
```

The `-n` flag specifies how many deployments to create. The `--create` flag indicates that these are *new* deployments, and we're not simply updating the settings of existing deployments. (If you ever want to apply changes to existing deployments, pass the `--update` flag instead.) The `--memory` flag tells OpenWhisk how much memory Docker should allocate to the JVM runtimes. Finally, the `--concurrency` flag tells OpenWhisk how many invocations each NameNode can process simultaneously. For additional details concerning the `whisk_helper_gcp.py` script and its arguments, execute the following:

``` sh
python3 /home/ubuntu/repos/hops/dev-support/whisk/whisk_helper_gcp.py --help
```

# ZooKeeper

The ZooKeeper virtual machines required by λFS are automatically created by the `create_aws_infrastructure.py` script. This script uses the public λFS ZooKeeper AMI to create the virtaul machines. The script also automatically updates their configuration. Optionally, the script will start the ZooKeeper cluster once it's been created. It can also populate the ZooKeeper cluster with the necessary `ZNode` structure required by λFS. 

We provide two utility scripts to start the ZooKeeper cluster once the VMs have already been created (i.e., if you create the cluster using the `create_aws_infrastructure.py` script and then shut down the virtaul machines). One of these scripts is `aws-setup/scripts/start-zk.sh`. This can be executed from your local computer (if your computer is capable of executing shell scripts). There is an equivalent script located in `/home/ubuntu/scripts/start-zk.sh` on the λFS primary client/experiment driver virtual machine/AMI. 

# MySQL Cluster NDB

The NDB virtual machines required by λFS are automatically created by the `create_aws_infrastructure.py` script. This script uses the public λFS NDB AMIs to create the NDB Manager Node and NDB Data Nodes. The script also automatically updates their configuration. Optionally, the script will start the NDB cluster once it's been created. It can also populate the NDB cluster with the necessary table structure (as required by λFS). 

As with ZooKeeper, we provide two utility scripts to start the MySQL NDB cluster once the VMs have already been created. One of these scripts is `aws-setup/scripts/start-ndb.sh`. This can be executed from your local computer (if your computer is capable of executing shell scripts). There is an equivalent script located in `/home/ubuntu/scripts/start-ndb.sh` on the λFS primary client/experiment driver virtual machine/AMI. 

To see what tables are created (and required) by HopsFS and λFS, you can see the SQL scripts used to populate the NDB cluster in `aws-setup/sql_scripts`. The `aws-setup/sql_scripts/serverless.sql` file contains the table definitions that are specific to λFS, while the other `.sql` files contain table definitions used by both λFS and HopsFS.

You can manually start the MySQL Cluster NDB Manager Node via:
``` sh
sudo /usr/local/bin/ndb_mgmd --skip-config-cache -f /var/lib/mysql-cluster/config.ini --reload
```

**NOTE:** If you're starting the NDB Manager Node for the very first time (i.e., with a new cluster), then you should add the `--initial` flag and omit the `--reload` flag as follows:
``` sh
sudo /usr/local/bin/ndb_mgmd --initial --skip-config-cache -f /var/lib/mysql-cluster/config.ini
```

You may need to restart the `mysql` service on the NDB Manager Node's VM after starting the NDB Manager Node via: 
``` sh
sudo service mysql restart
```

And you can manually start the MySQL Cluster NDB Data Nodes via: 
``` sh
sudo ndbmtd
```

If you're starting the NDB Data Node for the very first time (i.e., with a new cluster), then you should add the `--initial` flag to the `sudo ndbmtd ` command as follows:
``` sh
sudo ndbmtd --initial
```

# Basic Test 

In the home directory (`~/`, `/home/ubuntu/`) of the λFS AMI, there is a `BasicTest.jar` file. This can be used to perform a basic test to verify that the system is working.

To execute the `BasicTest.jar`, execute the following command from the `/home/ubuntu` directory:

``` sh
java -Dlog4j.configuration=file:resources/log4j.properties -cp ".:/home/ubuntu/BasicTest.jar:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*" com.gmail.benrcarver.distributed.BasicTest
```

This application will create an empty file named `"test.txt"` in the root directory of the λFS file system (so, the full path of the file is `/test.txt`). Then, the application will perform a `list` operation on the root directory `/`, listing the directory's contents. You should see the `test.txt` file when this list operation is performed.

If everything works, then you'll also see a message prefixed with `[SUCCESS]` before the application exits. If there's an error, then you'll see a message prefixed with `[ERROR]` (or you'll simply encounter some sort of Java exception).