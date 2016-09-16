#!/bin/bash

###
#	Run once with in priviledged mode
###

script_path=$(dirname "$(readlink -f "$0")")
cd $script_path

sudo apt-get install python3-pip

pip3 install pyyaml
pip3 install mysql-connector
pip3 install tendo