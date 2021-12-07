#!/bin/bash

DIR=/opt/Development/Projects/datp/datp-xpanse-dev/DATP
cd ${DIR}

export JUICE_CONFIG=file:::$(pwd)/test/unit/juice-config.json

# Initialise the SMS mock directory
#echo
#echo Clearing mock communications:
#mockComms=/tmp/convx-comms
#echo mock comms: ${mockComms}
#mkdir -p ${mockComms}
#rm -rf ${mockComms}/*

# Initialise the mock S3 directory
#echo Initialsing mock S3 files:
#mockS3=/opt/Development/Projects/convx/convx-config/local-server/volumes/filestore/unittest
#mockS3Initial=/opt/Development/Projects/convx/convx-config/local-server/volumes/filestore/unittest-startingPoint
#echo mock S3: ${mockS3}
#mkdir -p ${mockS3}
#rm -rf ${mockS3}/*
#cp -R ${mockS3Initial}/* ${mockS3}
#rm -rf /tmp/convx-s3/*
#echo
#find ${mockS3} -type f
#echo

# Run the test
for test in ${*} ; do
  echo Running test ${test}...

  npx ava --verbose test/unit/${test}
done

# Show the Mock directories
#find /opt/Development/Projects/convx/convx-config/local-server/volumes/filestore/unittest -type f
#find /tmp/convx-* -type f
