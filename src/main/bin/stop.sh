#!/bin/bash

cd `dirname $0`
cd ..
project_dir=`pwd`

if [ -f ${project_dir}/collector.pid ];then
   kill -15 `cat ${project_dir}/collector.pid`
   rm -rf ${project_dir}/collector.pid
fi
