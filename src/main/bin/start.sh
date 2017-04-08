#!/bin/bash

cd `dirname $0`
cd ..
project_dir=`pwd`
if [ -z $JAVA_HOME ]; then
 JAVA_HOME=/usr/local/jdk18
fi
conf_file=${project_dir}'/conf/kafka.properties'
echo $conf_file
$JAVA_HOME/bin/java -Dkafka.properties=$conf_file -cp  "./lib/*"  io.sugo.kafka.FileToKafka
