#!bin/sh 

# The first argument should be your account ID.
# The second argument is the IAM role name.

kubectl annotate serviceaccount ebs-csi-controller-sa \
    -n kube-system \
    eks.amazonaws.com/role-arn=arn:aws:iam::$1:role/$2