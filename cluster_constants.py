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
import xml_file
from xml_file import XML_File
import re

TESTING_MODE = False
IGNORE_ZK = False
MODIFY_CONFIG_FILES = True

# constants
CLUSTER_NAME = "mycluster"
USER_ID = os.getlogin()
DEFAULT_FS = "hdfs://"+CLUSTER_NAME
HOME_DIR = os.environ["HOME"]

HADOOP_HOME = os.environ["HADOOP_HOME"]
HADOOP_CONF_DIR = HADOOP_HOME+"/etc/hadoop"
HIVE_HOME = os.environ["HIVE_HOME"]

ZOOKEEPER_HOME = os.environ["ZOOKEEPER_HOME"]
ZOOKEEPER_CONF_DIR = ZOOKEEPER_HOME + "/conf"

SSH_KEYFILE_DIR = HOME_DIR + "/.ssh/id_rsa"

JOURNALNODE_PORT = "8485"
JOURNALNODE_EDITS_DIR = HADOOP_HOME+"/tmp/journalnode"

NAMENODE_RPC_PORT = "9000"
NAMENODE_HTTP_PORT = "50070"

ZK_CLIENT_PORT = "2181"
METASTORE_PORT = "9083"
HIVESERVER_PORT = "10001"
SSH_KEYFILE_DIR = HOME_DIR+"/.ssh/id_rsa"


#xml config files

hive_site_xml_path = HIVE_HOME+"/conf/hive-site.xml"
hive_site_xml = XML_File(hive_site_xml_path)
hdfs_site_xml_path = HADOOP_CONF_DIR+"/hdfs-site.xml"
hdfs_site_xml = XML_File(hdfs_site_xml_path)
core_site_xml_path = HADOOP_CONF_DIR+"/core-site.xml"
core_site_xml = XML_File(core_site_xml_path)
yarn_site_xml_path = HADOOP_CONF_DIR+"/yarn-site.xml"
yarn_site_xml = XML_File(yarn_site_xml_path)

if IGNORE_ZK:
    ZK_PORT = "44298"
else:
    ZK_PORT = "2181"

if not MODIFY_CONFIG_FILES:
    tgt_hive_site_xml = "tgt_hive_site.xml"
    tgt_hdfs_site_xml = "tgt_hdfs_site.xml"
    tgt_core_site_xml = "tgt_core_site.xml"
    tgt_yarn_site_xml = "tgt_yarn_site.xml"
else:
    tgt_hive_site_xml = hive_site_xml_path
    tgt_hdfs_site_xml = hdfs_site_xml_path
    tgt_core_site_xml = core_site_xml_path
    tgt_yarn_site_xml = yarn_site_xml_path

# globals
SLAVE_DAEMON_PORT = "8888"
