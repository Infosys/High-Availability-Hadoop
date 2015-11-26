#!/usr/bin/python

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


# This is the agent program that runs in every cluster node

import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import cluster_constants
from cluster_constants import *
import utils
from utils import *
import os
import subprocess
import re

def main():
    print ("init")
    server = SimpleXMLRPCServer(('0.0.0.0', int(SLAVE_DAEMON_PORT)))
    server.register_introspection_functions()
    server.register_instance(Node())
    print ("starting server")
    server.serve_forever()

class Node:
    def __init__(self):
        pass

    def backup_config_files(self):
        # tested
        for fil in get_conf_files():
            dirname = os.path.dirname(os.path.realpath(fil))
            src_fname = os.path.basename(os.path.realpath(fil))
            dst_fname = src_fname+".backup."+get_timestamp()
            backup_dir = dirname+"/backup"
            self.create_dir(backup_dir)
            cmd = "cp "+fil+" "+backup_dir+"/"+dst_fname

            get_result(cmd)

        return True

    def is_zkfc_running(self):
        # tested
        '''
        return true if ths zkfc is running and the state is active
        '''
        return is_process_running_by_name("org.apache.hadoop.hdfs.tools.DFSZKFailoverController")

    def stop_zkfc(self):
        # tested
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh stop zkfc"

        if self.is_zkfc_running():
            get_result(cmd)

        return True

    def start_zkfc(self):
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh start zkfc"

        if not self.is_zkfc_running():
            get_result(cmd)

        return True

    def clean_hadoop_temp_namenode_dir(self):
        # tested
        path = re.match("^(.*)/files/app/hadoop", HADOOP_HOME).group(1)+"/app/data/dfs/name"
        cmd = "rm -rf "+path
        get_result(cmd)
        return True

    def clean_hadoop_temp_datanode_dir(self):
        # tested
        path = re.match("^(.*)/files/app/hadoop", HADOOP_HOME).group(1)+"/app/data/dfs/data"
        cmd = "rm -rf "+path
        get_result(cmd)
        return True

    def clean_hadoop_temp_dirs(self):
        # tested
        self.clean_hadoop_temp_namenode_dir()
        self.clean_hadoop_temp_datanode_dir()
        return True

    def clean_journalnode_dir(self):
        # tested
        cmd="rm -rf "+JOURNALNODE_EDITS_DIR+"/*"
        get_result(cmd)
        return True

    def is_namenode_running(self):
        # tested
        '''
        return true if ths namenode is running and the state is active
        '''
        return is_process_running_by_name("org.apache.hadoop.hdfs.server.namenode.NameNode")

    def stop_namenode(self):
        # tested
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh stop namenode"

        if self.is_namenode_running():
            get_result(cmd)

        return True

    def start_namenode(self):
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh start namenode"

        if not self.is_namenode_running():
            get_result(cmd)

        return True

    def is_datanode_running(self):
        '''
        return true if ths datanode is running and the state is active
        '''
        return is_process_running_by_name("org.apache.hadoop.hdfs.server.datanode.DataNode")

    def stop_datanode(self):
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh stop datanode"

        if self.is_datanode_running():
            get_result(cmd)

        return True

    def start_datanode(self):
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh start datanode"

        if not self.is_datanode_running():
            get_result(cmd)

        return True
        
    def format_namenode(self):
        cmd = HADOOP_HOME+"/bin/hdfs namenode -format"
        get_result(cmd)
        return True
        
    def bootstrap_standby(self):
        # tested, idemp
        cmd = HADOOP_HOME+"/bin/hdfs namenode -bootstrapStandby -force -nonInteractive"
        get_result(cmd)
        return True
        
    def jn_initialize_shared_edits(self):
        # tested
        cmd = HADOOP_HOME+"/bin/hdfs namenode -initializeSharedEdits -force -nonInteractive"
        get_result(cmd)
        return True

    def is_journalnode_running(self):
        #tested
        return is_process_running_by_name("org.apache.hadoop.hdfs.qjournal.server.JournalNode")
        
    def start_journalnode(self):
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh start journalnode"

        if not self.is_journalnode_running():
            get_result(cmd)

        return True
        
    def stop_journalnode(self):
        cmd = HADOOP_HOME+"/sbin/hadoop-daemon.sh stop journalnode"

        if self.is_journalnode_running():
            get_result(cmd)

        return True

    def is_hiveserver_running(self):
        #tested
        return is_process_running_by_name("org.apache.hive.service.server.HiveServer2")

    def start_hiveserver(self):
        # tested
        cmd = HIVE_HOME+"/bin/hive --service hiveserver2 > "+HIVE_HOME+"/../../hive_log/hiveserver2.log"

        if not self.is_hiveserver_running():
            spawn(cmd)

        return True

    def stop_hiveserver(self):
        # tested
        if self.is_hiveserver_running():
            kill_by_name("org.apache.hive.service.server.HiveServer2")

        return True

    def update_host_for_hiveservers(self):
        hive_xml = XML_File(hive_site_xml_path)
        hive_xml.set_property("hive.server2.thrift.bind.host", get_current_hostname())
        hive_xml.write(hive_site_xml_path)
        return True

    def get_zk_nodes(self):
        # tested
        '''
         returns list of server ip addresses of the zk ensemble
          [zkserver-ip1, zkserver-ip2, ...]
        '''
        conf_file = ZOOKEEPER_CONF_DIR + "/zoo.cfg"
        cmd = "grep  ^server\.  " + conf_file + " | cut -f2 -d= | cut -f1 -d:"

        return [get_ipaddr(x) for x in get_result(cmd)[0]]


    def format_ZK(self):
        cmd = "hdfs zkfc -formatZK -force -nonInteractive"
        get_result(cmd)
        return True

    def is_zookeeper_running(self):
        return is_process_running_by_name("org.apache.zookeeper.server.quorum.QuorumPeerMain")

    def start_zookeeper(self):
        cmd = ZOOKEEPER_HOME + "/bin/zkServer.sh start"

        if not self.is_zookeeper_running():
            get_result(cmd)

        return True

    def stop_zookeeper(self):
        if self.is_zookeeper_running():
            cmd = ZOOKEEPER_HOME + "/bin/zkServer.sh stop"
            get_result(cmd)

        return True

    def passwordless_ssh_works(self, target):
        #tested
        pipe = subprocess.Popen("ssh -oNumberOfPasswordPrompts=0 "+target+" \"echo hello\"", shell=True)
        if pipe.wait() == 0:
            return True
        else:
            return False

    def start_hadoop_services(self):
        assert(get_hostname(MASTER_NODE) == get_current_hostname())
        cmd = HADOOP_HOME+"/sbin/start-dfs.sh"
        get_result(cmd)

        cmd = HADOOP_HOME+"/sbin/start-yarn.sh"
        get_result(cmd)
        return True

    def stop_hadoop_services(self):
        assert(get_hostname(MASTER_NODE) == get_current_hostname())
        cmd = HADOOP_HOME+"/sbin/stop-yarn.sh"
        get_result(cmd)

        cmd = HADOOP_HOME+"/sbin/stop-dfs.sh"
        get_result(cmd)
        return True

    def is_metastore_running(self):
        # tested
        return is_process_running_by_name("org.apache.hadoop.hive.metastore.HiveMetaStore")

    def start_metastore(self):
        cmd = HIVE_HOME+"/bin/hive --service metastore > "+HIVE_HOME+"/../../hive_log/metastore.log"

        if not self.is_metastore_running():
            spawn(cmd)

        return True

    def stop_metastore(self):
        if self.is_metastore_running():
            kill_by_name("org.apache.hadoop.hive.metastore.HiveMetaStore")

        return True

    def is_resourcemanager_running(self):
        # tested
        return is_process_running_by_name("org.apache.hadoop.yarn.server.resourcemanager.ResourceManager")

    def start_resourcemanager(self):
        cmd = HADOOP_HOME + "/sbin/yarn-daemon.sh start resourcemanager"

        if not self.is_resourcemanager_running():
            get_result(cmd)

        return True

    def stop_resourcemanager(self):
        if self.is_resourcemanager_running():
            cmd = HADOOP_HOME + "/sbin/yarn-daemon.sh stop resourcemanager"
            get_result(cmd)

        return True

    def create_dir(self, direc):
        # tested
        if not os.path.isdir(direc):
            os.mkdir(direc)

        return True


if __name__ == "__main__":
    print ("starting")
    main()

