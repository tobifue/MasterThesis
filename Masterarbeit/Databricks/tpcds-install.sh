#!/bin/bash
sudo apt-get update
sudo apt-get --assume-yes install gcc make flex bison byacc git

cd /mnt
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
