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

import os
import socket
import signal
import subprocess
import re
from cluster_constants import *

#misc util functions

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def read_stream(f):
    #tested
    op = list()
    l = f.readline()
    while (l):
        op.append(l.strip())
        l = f.readline()
    return op

def get_curr_time():
    # tested
    return get_result("date")[0][0]

def get_timestamp():
    # tested
    return get_result("date +%H%M%S%p%d%b%Y")[0][0]

def log(s):
    # tested
    curr_time=get_curr_time()
    print curr_time+":"+s

def warn(s):
    print bcolors.WARNING+s+bcolors.ENDC

def prompt_user(s):
    print bcolors.OKBLUE+s+bcolors.ENDC,

def normal_msg(s):
    print bcolors.OKGREEN+s+bcolors.ENDC

def insight(s):
    print bcolors.OKGREEN+s+bcolors.ENDC

def read_file(fname):
    # tested
    return read_stream(open(fname))

def spawn(cmd):
    # tested
    # for async launch. For sync execution, to use get_result
    subprocess.Popen(cmd, shell=True)

def get_result(cmd):
    # tested
    pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return (read_stream(pipe.stdout), read_stream(pipe.stderr))

def die (s):
    # tested
    print bcolors.FAIL+s+bcolors.ENDC
    exit (1)

def split_strip(ip_str, delim=","):
    # tested
    return [x.strip() for x in ip_str.split(delim)]

def get_ipaddr(ip_str):
    # tested
    try:
        socket.inet_pton(socket.AF_INET, ip_str)
        return ip_str
    except socket.error:
        return socket.gethostbyname(ip_str)

def get_hostname(ip_str):
    # tested
    try:
        socket.inet_pton(socket.AF_INET, ip_str)
        return socket.gethostbyaddr(ip_str)[0]
    except socket.error:
        return ip_str

def is_process_running_by_name(ip_str):
    # tested
    for pid in get_pid(ip_str):
        return True

    return False

def kill_by_name (ip_str):
    # tested
    for pid in get_pid(ip_str):
        print "killing ", pid
        os.kill(int(pid), signal.SIGKILL)

def get_pid(ip_str):
    # tested
    assert(len(ip_str)>0)
    #cmd = "ps -eo %p%c%a|grep -i ["+ip_str[0]+"]"+ip_str[1:]+" | awk '{print $1}'"
    cmd = "pgrep -f "+ip_str
    return get_result(cmd)[0]

def get_current_hostname():
    # tested
    return get_hostname(socket.gethostbyname(socket.gethostname()))

def get_conf_files():
    # tested
    files = [HADOOP_CONF_DIR+"/hdfs-site.xml", HADOOP_CONF_DIR+"/core-site.xml" ,HADOOP_CONF_DIR+"/yarn-site.xml", HIVE_HOME+"/conf/hive-site.xml"]
    return files

