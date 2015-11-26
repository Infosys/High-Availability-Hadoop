#!/bin/bash

# The MIT License (MIT)

# Copyright (c) 2015 Infosys Ltd

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


# Driver shell script

slave_file=$HADOOP_CONF_DIR/slaves
curr_dir=`pwd`
curr_id=`whoami`
curr_time=`date  +%H%M%S%p%d%b%Y`
this_host=`hostname`

clear
echo "Initializing..."
this_host_ip=`ping -w1 ${this_host} | head -1 | awk '{print $3}' | tr -d "()"`

# Start agent processes
for slave in `cat $slave_file`
do
    slave_ip=`ping -w1 ${slave} | head -1 | awk '{print $3}' | tr -d "()"`

    if [[ $slave_ip != $this_host_ip ]]
    then
        echo "Setting up password-less ssh from ${this_host} to ${slave}"
        ssh-copy-id -i ~/.ssh/id_rsa.pub ${slave}
    fi

    #echo "copying files to ${slave}"
    scp $curr_dir/*py $curr_id@${slave}:$curr_dir >/dev/null 
    #echo "starting slave daemon in ${slave}"
    ssh -q $curr_id@${slave} << END
cd $curr_dir
if [[ ! -d $curr_dir/logs ]]
then
mkdir $curr_dir/logs
fi
chmod a+x slave_daemon.py
./slave_daemon.py 2>&1 | tee logs/slave_log_${curr_time}.txt > /dev/null &
END

done

echo "Starting HA setup program"
chmod a+x ha_setup.py
./ha_setup.py

#echo "Shut down slave processes"

for slave in `cat $slave_file`
do
    #echo "stopping slave in ${slave}"
    ssh -q $curr_id@${slave} << END
cd $curr_dir
pkill slave_daemon
END

done

