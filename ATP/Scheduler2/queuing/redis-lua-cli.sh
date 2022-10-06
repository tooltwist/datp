#!/bin/bash

export JUICE_CONFIG=file:::/opt/Development/Projects/datp/datp-xpanse-dev/util/Config/config-native-dev-slave1.json
#echo node -r esm redis-lua-cli.js $*
     node -r esm redis-lua-cli.js $*
