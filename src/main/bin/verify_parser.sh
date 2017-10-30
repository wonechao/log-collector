#!/bin/sh

project_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
JAVA=$JAVA_HOME'/bin/java'
if [ -z $JAVA_HOME ]; then
 JAVA='java'
fi
conf_file=${project_dir}'/conf/collect.properties'
echo $@
$JAVA -Dcollect.properties=$conf_file -cp  "${project_dir}/lib/*:${project_dir}/conf/"  io.sugo.collect.GrokVerification $@

