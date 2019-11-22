#!/bin/bash

MYPWD=`pwd`
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

cd ${SCRIPTPATH}
java -cp '../modules/*:../lib/*:../services/*:../plugins/*' org.esa.sen4cap.ServicesStartup

cd ${MYPWD}


