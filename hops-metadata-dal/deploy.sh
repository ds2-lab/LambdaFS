#!/bin/bash

if [ $# -ne 1 ] ; then
 echo "usage: $0 hop-dal-version"
 exit 1
fi


version=$1
proj=${PWD##*/}
mvn  deploy:deploy-file -Durl=scpexe://kompics.i.sics.se/home/maven/repository \
                       -DrepositoryId=sics-release-repository \
                       -Dfile=./target/$proj-$version.jar \
                       -DgroupId=se.sics.hop.metadata.$proj \
                       -DartifactId=$proj \
                       -Dversion=$version \
                       -Dpackaging=jar \
                       -DpomFile=./pom.xml \
                       -DgeneratePom.description="Hop DAL Interface" \
