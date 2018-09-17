#!/bin/bash

set -e 

# We need tarantool 2.0 for jdbc/sql.
curl http://download.tarantool.org/tarantool/2.0/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`

sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
sudo tee /etc/apt/sources.list.d/tarantool_2.0.list <<- EOF
deb http://download.tarantool.org/tarantool/2.0/ubuntu/ $release main
deb-src http://download.tarantool.org/tarantool/2.0/ubuntu/ $release main
EOF

sudo apt-get update
sudo apt-get -y install tarantool tarantool-common

sudo cp src/test/instance.lua /etc/tarantool/instances.enabled/jdk-testing.lua
sudo tarantoolctl stop  example
sudo tarantoolctl start jdk-testing
