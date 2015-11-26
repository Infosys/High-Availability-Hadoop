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

#!/usr/bin/python

# Provides an abstraction of the Hadoop cluster

import os
from cluster_constants import *
from servers import *
from utils import *
from xml_file import XML_File
import re
import xmlrpclib
import itertools

class Cluster():

    def __init__ (self):
        self.slaves = self.get_slaves()
        self.slave_daemons = self.get_slave_daemons(self.slaves) # dict: ipaddress -> daemon
        self.pre_ha_namenode = self.get_pre_ha_namenode()
        log("Validating config params...")
        self.validate_config()

    def get_slaves(self):
        if os.path.isfile(HADOOP_CONF_DIR+"/slaves"):
            return [get_hostname(x) for x in read_file(HADOOP_CONF_DIR+"/slaves")]
        else:
            die("Unable to locate slaves file in "+HADOOP_CONF_DIR+": Exiting.")

    def get_slave_daemons(self, slaves):
        op = dict()
        for slave in slaves:
            op[slave] = xmlrpclib.ServerProxy("http://"+slave+":"+SLAVE_DAEMON_PORT)

        return op

    def get_pre_ha_namenode(self):

        return self.master()

        for node in self.namenodes():
            if self.slave_daemons[node].is_namenode_running():
                return node

        die ("No namenode currently executing. Aborting")

    def master(self):
        return get_hostname(MASTER_NODE)

    def namenodes(self):
        return [get_hostname(x) for x in split_strip(HA_NAMENODE_LIST)]

    def rms(self):
        return [get_hostname(x) for x in split_strip(HA_RM_LIST)]

    def hiveservers(self):
        return [get_hostname(x) for x in split_strip(HA_HIVESERVER2_LIST)]

    def metastores(self):
        return [get_hostname(x) for x in split_strip(HA_METASTORE_LIST)]

    def journalnodes(self):
        return [get_hostname(x) for x in split_strip(HA_JOURNALNODE_LIST)]

    def zknodes(self):
        return [get_hostname(x) for x in split_strip(HA_ZOOKEEPER_LIST)]

    def stop_zkfc(self):
        for node in self.namenodes():
            self.slave_daemons[node].stop_zkfc()

    def start_zkfc(self):
        for node in self.namenodes():
            self.slave_daemons[node].start_zkfc()

    def stop_zknodes(self):
        for node in self.zknodes():
            self.slave_daemons[node].stop_zookeeper()

    def start_zknodes(self):
        for node in self.zknodes():
            self.slave_daemons[node].start_zookeeper()

    def stop_datanodes(self):
        for node in self.slaves:
            self.slave_daemons[node].stop_datanode()

    def start_datanodes(self):
        for node in self.slaves:
            self.slave_daemons[node].start_datanode()

    def stop_metastores(self):
        for node in self.metastores():
            self.slave_daemons[node].stop_metastore()

    def start_metastores(self):
        for node in self.metastores():
            self.slave_daemons[node].start_metastore()

    def stop_hiveservers(self):
        for node in self.hiveservers():
            self.slave_daemons[node].stop_hiveserver()

    def start_hiveservers(self):
        for node in self.hiveservers():
            print ("starting hiveserver2 in ", node)
            self.slave_daemons[node].start_hiveserver()

    def update_host_for_hiveservers(self):
        for node in self.hiveservers():
            self.slave_daemons[node].update_host_for_hiveservers()

    def validate_config(self):
        # validate if this script is being run from the master node.
        if get_current_hostname() != self.master():
            die ("Please execute this script from the master node. Aborting")

        log("Starting Zookeeper")
        self.start_zknodes()

        #validate the HA configuration file

        def validate_node_list (node_list, node_type):
            for node in node_list:
                if node not in self.slaves:
                    die ("Please fix config file. Not a valid "+node_type+":"+node )

            if len(set(node_list)) < len(node_list):
                die ("Duplicate nodes found in node list for "+node_type+". Aborting.")

            if len(set(node_list)) < 2:
                die ("Insufficient number of nodes specified for "+node_type+" . Aborting")

            if node_type=="name node" and (self.pre_ha_namenode not in node_list):
                die ("Current name node "+self.pre_ha_namenode+" is not part of name node list. Please fix")

            if node_type=="name node" and len(node_list)!=2:
                die ("Please specify exactly two nodes for namenode")

            if node_type=="journalnode" and (len(set(node_list))<3 or len(set(node_list))%2==0):
                warn("Odd number of jounal nodes (minimum 3) is STRONGLY RECOMMENDED to ensure successful failover to standby in the event of namenode failover.")
                prompt_user("Do you still want to continue (y/n) ?: ")
                if raw_input().upper() != "Y":
                    die("Exiting")

        log("Validating node list")
        validate_node_list(self.namenodes(), "name node")
        validate_node_list(self.rms(), "resource manager")
        validate_node_list(self.hiveservers(), "hive server2")
        validate_node_list(self.metastores(), "metastore")
        validate_node_list(self.journalnodes(), "journalnode")
        validate_node_list(self.zknodes(), "zookeeper")

        # 9-Oct-2015: Commenting below checks for passwordless-ssh, since explicit fencing is not needed for zookeeper based HA

        # validate password-less login from the master node to all other nodes. Needed to broadcast modified config files

        #log("Validating password-less login")
        #for node in set(self.slaves).difference({self.master()}):
        #    if not self.slave_daemons[self.master()].passwordless_ssh_works(node):
        #        die ("Please enable password less ssh from master node to all other cluster nodes. Failed when trying between "+self.master()+" and "+node)


        #validate password-less login across machines identified for name node

        #for (nodeA, nodeB) in itertools.combinations(self.namenodes(), 2):
        #    if not self.slave_daemons[nodeA].passwordless_ssh_works(nodeB):
        #        die("Aborting. passwordless ssh needs to be set up from "+nodeA+" to "+nodeB)

        #    if not self.slave_daemons[nodeB].passwordless_ssh_works(nodeA):
        #        die("Aborting. passwordless ssh needs to be set up from "+nodeB+" to "+nodeA)

        log("Validations completed successfully")

        log("Storing backup of configuration files under corresponding /backup sub-directory in cluster machines")
        self.backup_config_files()

    def backup_config_files(self):
        for slave in self.slaves:
            self.slave_daemons[slave].backup_config_files() 

        return

    def broadcast_config(self):
        for slave in set(self.slaves).difference({self.master()}):
            for fil in get_conf_files():
                self.send_file(fil, slave)

    def send_file(self, fname, target):
        cmd = "scp "+fname+" "+USER_ID+"@"+target+":"+fname
        get_result(cmd)
        return

    def start_hadoop_services(self):
        self.slave_daemons[self.master()].start_hadoop_services()

        for node in set(self.rms()).difference(set(self.master())):
            self.slave_daemons[node].start_resourcemanager()

        for node in self.metastores():
            self.slave_daemons[node].start_metastore()

        for node in self.hiveservers():
            self.slave_daemons[node].start_hiveservers()

    def stop_hadoop_services(self):
        for node in set(self.rms()).difference(set(self.master())):
            self.slave_daemons[node].stop_resourcemanager()

        for node in self.hiveservers():
            self.slave_daemons[node].stop_hiveservers()

        for node in self.metastores():
            self.slave_daemons[node].stop_metastore()

        self.slave_daemons[self.master()].stop_hadoop_services()

    def check_journalnode_dir (self,jdir, nodes):
        for node in nodes:
            self.slave_daemons[node].create_dir(jdir)

    def format_ZK(self):
        self.slave_daemons[self.namenodes()[0]].format_ZK()

    def get_zk_ensemble(self,delimiter=","):
        return delimiter.join([zknode+":"+ZK_CLIENT_PORT for zknode in self.zknodes()])

    def is_zookeeper_running(self):
        for node in self.zknodes():
            if self.slave_daemons[node].is_zookeeper_running():
                return True

        return False

    def stop_namenodes(self):
        for node in self.namenodes():
            self.slave_daemons[node].stop_namenode()

    def get_standby_namenode(self):
        for node in self.namenodes():
            if node != self.pre_ha_namenode:
                return node

        die ("No standby namenode found. Aborting")
        
    def start_rms(self):
        for node in self.rms():
            self.slave_daemons[node].start_resourcemanager()
    
    def stop_rms(self):
        for node in self.rms():
            self.slave_daemons[node].stop_resourcemanager()

    def start_journalnodes(self):
        for node in self.journalnodes():
            self.slave_daemons[node].start_journalnode()
    
    def stop_journalnodes(self):
        for node in self.journalnodes():
            self.slave_daemons[node].stop_journalnode()

    def clean_hadoop_temp_dirs(self):
        for node in self.slaves:
            self.slave_daemons[node].clean_hadoop_temp_dirs()

    def clean_journalnode_dir(self):
        for node in self.slaves:
            self.slave_daemons[node].clean_journalnode_dir()

    def get_FSRoot(self):
        cmd = HIVE_HOME+"/bin/hive --service metatool -listFSRoot"
        return get_result(cmd)[0][2]

    def is_namenode_in_ha(self):
        if hdfs_site_xml.property_exists("dfs.nameservices"):
            return True
        else:
            return False

    def is_rm_in_ha(self):
        if yarn_site_xml.get_property("yarn.resourcemanager.ha.enabled")=="true":
            return True
        else:
            return False

    def is_hiveserver2_in_ha(self):
        if hive_site_xml.get_property("fs.default.name")=="hdfs://"+CLUSTER_NAME:
            if hive_site_xml.get_property("hive.server2.support.dynamic.service.discovery")=="true":
                return True
            else:
                return False
        else:
            return False

    def is_metastore_in_ha(self):
        if self.get_FSRoot() == "hdfs://"+CLUSTER_NAME:
            return True
        else:
            return False

    def namenode_ha(self):
        namenodes = self.namenodes()

        if self.is_namenode_in_ha():
            warn("Namenode HA enabled already.")
            msg = "Redo Namenode HA setup ? (Y/N):"
        else:
            insight("Namenode HA is not currently enabled")
            msg = "Proceed with changes for Namenode HA (Y/N):"

        prompt_user(msg)
        if raw_input().strip().upper() != "Y":
            log ("Aborting Namenode HA")
            return

        log ("Updating hdfs-site.xml for Name node HA")
        # update hdfs-site.xml
        props = dict()

        props["dfs.ha.automatic-failover.enabled"]="true"
        props["dfs.nameservices"]=CLUSTER_NAME

        namenode_str=",".join(["nn"+str(i+1) for i in range(len(namenodes))])
        props["dfs.ha.namenodes."+CLUSTER_NAME]=namenode_str

        shared_edits_dir = "qjournal://"+";".join([x+":"+JOURNALNODE_PORT for x in self.journalnodes()])+"/"+CLUSTER_NAME
        props["dfs.namenode.shared.edits.dir"]=shared_edits_dir

        props["dfs.client.failover.proxy.provider."+CLUSTER_NAME]="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        #props["dfs.ha.fencing.methods"]="sshfence" #split-brain scenario not possible using QJM: http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html#Configuration_details

        props["dfs.ha.fencing.methods"]="shell(/bin/true)"
        props["dfs.ha.fencing.ssh.private-key-files"]=SSH_KEYFILE_DIR
        props["fs.defaultFS"]=DEFAULT_FS

        for i in range(len(namenodes)):
            key = "dfs.namenode.rpc-address."+CLUSTER_NAME+".nn"+str(i+1)
            value = namenodes[i]+":"+NAMENODE_RPC_PORT
            props[key]=value

            key = "dfs.namenode.http-address."+CLUSTER_NAME+".nn"+str(i+1)
            value = namenodes[i]+":"+NAMENODE_HTTP_PORT
            props[key]=value

        hdfs_site_xml.bulk_update(props, tgt_hdfs_site_xml)

        log ("Updating core-site.xml for Name node HA")
        # update core-site.xml
        props = dict()
        props["fs.defaultFS"]=DEFAULT_FS
        props["ha.zookeeper.quorum"]=self.get_zk_ensemble()

        # check presence of jounal node edits directory. If not present, create the same 
        self.check_journalnode_dir(JOURNALNODE_EDITS_DIR, self.journalnodes())
        props["dfs.journalnode.edits.dir"]=JOURNALNODE_EDITS_DIR

        core_site_xml.bulk_update(props, tgt_core_site_xml)

        log("Publishing config changes for name node HA ")

        self.broadcast_config()

        # create node in ZK for namenode failover
        log("Creating node in ZK for name node failover controller ")
        self.format_ZK()
        
        log("Stopping all data nodes")
        self.stop_datanodes()

        log("Stopping name nodes")
        self.stop_namenodes()

        #start journal node daemons
        log("Stopping journal nodes")
        self.stop_journalnodes()
        log("Clearing journal node temp dir...")
        self.clean_journalnode_dir()
        log("Starting Journal nodes")
        self.start_journalnodes()

        warn("You will lose existing data if you choose to format current namenode.")
        prompt_user("Press Y to format current name node at "+self.pre_ha_namenode+": ")
        if raw_input().strip().upper() == "Y":
            self.clean_hadoop_temp_dirs()
            self.slave_daemons[self.pre_ha_namenode].format_namenode()
        
        log("Initializing journal nodes for shared edits")
        self.slave_daemons[self.pre_ha_namenode].jn_initialize_shared_edits()

        log("Starting primary NN1 namenode: "+self.pre_ha_namenode)
        self.slave_daemons[self.pre_ha_namenode].start_namenode()

        log("Bootstrapping standby NN2 namenode: "+self.get_standby_namenode())
        self.slave_daemons[self.get_standby_namenode()].clean_hadoop_temp_namenode_dir()
        self.slave_daemons[self.get_standby_namenode()].bootstrap_standby()

        log("Starting standby NN2 namenode")
        self.slave_daemons[self.get_standby_namenode()].start_namenode()

        log("Starting data nodes")
        self.start_datanodes()

        log("Starting zookeeper failover controllers in NN1 and NN2")
        self.start_zkfc()

    def hiveserver2_ha(self):
        props = dict()
        props["fs.default.name"]="hdfs://"+CLUSTER_NAME
        props["hive.zookeeper.quorum"]=self.get_zk_ensemble()
        props["hive.zookeeper.session.timeout"]="600000"
        props["hive.server2.support.dynamic.service.discovery"]="true"
        props["hive.server2.zookeeper.namespace"]="hiveserver2_zookeeper"
        props["hive.server2.thrift.bind.host"]="0.0.0.0"
        props["hive.server2.thrift.port"]=HIVESERVER_PORT

        if self.is_hiveserver2_in_ha():
            warn("Hiveserver2 HA enabled already.")
            msg = "Redo Hiveserver2 HA ? (Y/N): "
        else:
            insight("Hiveserver2 is currently not HA enabled.")
            msg = "Proceed with changes for Hiveserver2 HA ? (Y/N): "

        prompt_user(msg)
        resp = raw_input().strip()

        if resp.upper()=="Y":
            log("Changing config files")
            hive_site_xml.bulk_update(props, tgt_hive_site_xml)
            self.broadcast_config()
            self.update_host_for_hiveservers()
            log("Stopping Hiveserver(s)")
            self.stop_hiveservers()
            log("Starting Hiveservers")
            self.start_hiveservers()
            log("Hiveserver2 HA Completed")
            return
        else:
            log ("Aborting Hiveserver2 HA")
            return

    def metastore_ha(self):

        if self.is_metastore_in_ha():
            warn("Metastore HA is enabled already.")
            prompt_user("Redo Metastore HA? (y/n):")
            resp = raw_input().strip().upper()

            if resp != "Y":
                log("Aborting Metastore HA")
                return
        else:
            insight("Metastore is currently not HA enabled.")

        props = dict()
        metastore_ensemble = ",".join(["thrift://"+x+":"+METASTORE_PORT for x in self.metastores()])
        props["hive.metastore.uris"]=metastore_ensemble

        cmd = "hive --service metatool -updateLocation hdfs://"+CLUSTER_NAME+" "+self.get_FSRoot()
        log("Please Validate output of metatool update location dryrun & confirm :")
        normal_msg("\n"+"\n".join(get_result(cmd+" -dryRun")[0]))

        prompt_user("proceed with changes for Metastore HA (y/n):")
        resp = raw_input().strip().upper()

        if resp=="Y":
            log("Changing config files")
            hive_site_xml.bulk_update(props, tgt_hive_site_xml)
            self.broadcast_config()
            log("Stopping metastore(s)")
            self.stop_metastores()
            log("Updating metastore FS Root")
            get_result(cmd)[0][0]
            log("Starting metastores")
            self.start_metastores()
            log("Metastore HA Completed")
            return
        else:
            log ("Aborting Metastore HA")
            return

    def rm_ha(self):

        props = dict()
        props["yarn.resourcemanager.ha.enabled"]="true"
        props["yarn.resourcemanager.recovery.enabled"]="true"
        props["yarn.resourcemanager.ha.automatic-failover.enabled"]="true"

        no_rm = len(self.rms())
        rm_str = ",".join(["rm"+str(i) for i in range(1,no_rm+1)])

        props["yarn.resourcemanager.ha.rm-ids"]=rm_str
        for (cnt, node) in enumerate(self.rms(),1):
            props["yarn.resourcemanager.hostname.rm"+str(cnt)]=node

        props["yarn.resourcemanager.zk-address"]=self.get_zk_ensemble()
        props["yarn.resourcemanager.ha.automatic-failover.zk-base-path"]="/yarn-leader-election"
        props["yarn.resourcemanager.cluster-id"]=CLUSTER_NAME

        if self.is_rm_in_ha():
            warn("Resource manager HA is enabled already.")
            msg = "Redo Resource manager HA ? (Y/N):"
        else:
            insight("Resource manager is currently not HA enabled.")
            msg = "proceed with changes for ResourceManager HA (y/n):"

        prompt_user(msg)
        resp = raw_input().strip()

        if resp.upper()=="Y":
            log("Changing config files")
            yarn_site_xml.bulk_update(props, tgt_yarn_site_xml)
            self.broadcast_config()
            log("Stopping ResourceManager(s)")
            self.stop_rms()
            log("Starting ResourceManagers")
            self.start_rms()
            log("ResourceManager HA Completed")
            return
        else:
            log ("Aborting ResourceManager HA")
            return
