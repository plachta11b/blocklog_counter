#!/bin/bash

###
#	Start recomended every 5 minutes (best by cron)
###

script_path=$(dirname "$(readlink -f "$0")")
cd $script_path


python3 $script_path/src/block_counter.py