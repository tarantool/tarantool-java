#!/bin/bash

set -e

curl http://download.tarantool.org/tarantool/1.9/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`

sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
sudo tee /etc/apt/sources.list.d/tarantool_1.9.list <<- EOF
deb http://download.tarantool.org/tarantool/1.9/ubuntu/ $release main
deb-src http://download.tarantool.org/tarantool/1.9/ubuntu/ $release main
EOF

sudo apt-get update
sudo apt-get -y install tarantool tarantool-common

sudo tarantoolctl stop example
