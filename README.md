Hadoop HA Automation
====================

Hadoop HA Automation is a utility to automate the HA setup for
   - Namenode
   - Resource Manager
   - Hive Metastore
   - Hiveserver2

Usage
=====

Place the source files in the Hadoop master node directory path. Ensure that the same directory path is available 
in each of the slave node with read-write access. For example, if you place the source files in /home/hadoop/HA folder 
in the master node, also ensure that every slave node has /home/hadoop/HA with read-write access for the user id

Before execution, edit the constant values in servers.py to customize the utility for your Hadoop installation.

'ha_setup.sh' is the entry point for the utility.  The utility works by starting agent processes in the 
cluster nodes. 'ha_setup.sh' first starts the slave(agent) processes and then invokes the HA setup program ('ha_setup.py'). 
Lastly as cleanup, the agent processes get killed.

The agent processes communicate with the driver program ('ha_setup.py') by way of RPC on port 8888.  If you wish to
modify the port, you can change the same (SLAVE_DAEMON_PORT) in cluster_constants.py

The file cluster_constants.py contains the values of various parameters. Default values are provided as much as possible
for each configuration parameter. If your Hadoop installation uses any non-standard configuration, 
you may want to change the corresponding parameter in cluster_constants.py


Dependencies
============

1) The utility depends on valid values for below environment variables. Please verify the same before execution.
   - HOME
   - HADOOP_HOME
   - HIVE_HOME
   - ZOOKEEPER_HOME 

2) Setup passwordless SSH from the master node to each of the slave node. It is a simple procedure and below link tells 
how to go about the same

http://www.thegeekstuff.com/2008/11/3-steps-to-perform-ssh-login-without-password-using-ssh-keygen-ssh-copy-id/

3) Create directories in each slave node identical to the master node directory with the source code. For example, if 
you use 'hadoop' userid and the source files are in /home/hadoop/HA, create the folder /home/hadoop/HA in each slave node 
with read-write access for 'hadoop' user id.

4) This utility requires Python 2.7.5 or above in all the Hadoop cluster nodes

This code is shared under the MIT License
