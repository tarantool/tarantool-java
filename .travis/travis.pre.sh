#!/bin/bash

set -exuo pipefail # Strict shell

curl http://download.tarantool.org/tarantool/${TNT_VERSION}/gpgkey | sudo apt-key add -
RELEASE=`lsb_release -c -s`

sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
sudo tee /etc/apt/sources.list.d/tarantool_${TNT_VERSION/./_}.list <<- EOF
deb http://download.tarantool.org/tarantool/${TNT_VERSION}/ubuntu/ ${RELEASE} main
deb-src http://download.tarantool.org/tarantool/${TNT_VERSION}/ubuntu/ ${RELEASE} main
EOF

sudo apt-get update
sudo apt-get -y install tarantool tarantool-common

sudo tarantoolctl stop example
