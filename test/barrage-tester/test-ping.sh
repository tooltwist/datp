#!/bin/bash

DIR=$(cd $(dirname $0); pwd);

export LOADTEST_URL=http://localhost:4000
export LOADTEST_CONNECTIONS=2
# export LOADTEST_DURATION=30
# export LOADTEST_AMOUNT=10

# echo "$" node -r esm main.js
#          node -r esm main.js

echo "$" node test-ping.js
         node test-ping.js
