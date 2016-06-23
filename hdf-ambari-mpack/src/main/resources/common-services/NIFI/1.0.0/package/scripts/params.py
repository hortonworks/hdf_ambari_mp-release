#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script
import sys, os, glob
from resource_management.libraries.functions.default import default


    
# server configurations
config = Script.get_config()
stack_version_buildnum = default("/commandParams/version", None)

nifi_install_dir = '/usr/hdf/current/nifi'

# params from nifi-ambari-config
nifi_initial_mem = config['configurations']['nifi-ambari-config']['nifi.initial_mem']
nifi_max_mem = config['configurations']['nifi-ambari-config']['nifi.max_mem']
nifi_ambari_reporting_frequency = config['configurations']['nifi-ambari-config']['nifi.ambari_reporting_frequency']

nifi_node_port = config['configurations']['nifi-ambari-config']['nifi.node.port']
nifi_node_protocol_port = config['configurations']['nifi-ambari-config']['nifi.node.protocol.port']

nifi_znode = config['configurations']['nifi-ambari-config']['nifi.nifi_znode']
nifi_authorizer = config['configurations']['nifi-ambari-config']['nifi.nifi_authorizer']
  
master_configs = config['clusterHostInfo']

nifi_num_nodes = len(master_configs['nifi_master_hosts'])
if nifi_num_nodes > 1:
  nifi_is_node='true'
else:
  nifi_is_node='false'  
nifi_node_hosts = ",".join(master_configs['nifi_master_hosts'])



nifi_node_dir=nifi_install_dir
 
conf_dir = os.path.join(*[nifi_node_dir,'conf'])
bin_dir = os.path.join(*[nifi_node_dir,'bin'])
work_dir = os.path.join(*[nifi_node_dir,'work'])

# params from nifi-env
nifi_user = config['configurations']['nifi-env']['nifi_user']
nifi_group = config['configurations']['nifi-env']['nifi_group']

nifi_node_log_dir = config['configurations']['nifi-env']['nifi_node_log_dir']
nifi_node_log_file = os.path.join(nifi_node_log_dir,'nifi-setup.log')

# params from nifi-boostrap
nifi_env_content = config['configurations']['nifi-env']['content']


# params from nifi-logback
nifi_master_logback_content = config['configurations']['nifi-master-logback-env']['content']
nifi_node_logback_content = config['configurations']['nifi-node-logback-env']['content']

# params from nifi-properties-env
nifi_master_properties_content = config['configurations']['nifi-master-properties-env']['content']
nifi_node_properties_content = config['configurations']['nifi-node-properties-env']['content']
  
# params from nifi-flow
nifi_flow_content = config['configurations']['nifi-flow-env']['content']

# params from nifi-state-management-env
nifi_state_management_content = config['configurations']['nifi-state-management-env']['content']

# params from nifi-boostrap
nifi_boostrap_content = config['configurations']['nifi-bootstrap-env']['content']


#autodetect jdk home
jdk64_home=config['hostLevelParams']['java_home']

#autodetect ambari server for metrics
if 'metrics_collector_hosts' in config['clusterHostInfo']:
  metrics_collector_host = str(config['clusterHostInfo']['metrics_collector_hosts'][0])
  metrics_collector_port = str(get_port_from_url(config['configurations']['ams-site']['timeline.metrics.service.webapp.address']))
else:
  metrics_collector_host = ''
  metrics_collector_port = ''


#detect zookeeper_quorum
zookeeper_port=default('/configurations/zoo.cfg/clientPort', None)
#get comma separated list of zookeeper hosts from clusterHostInfo
index = 0 
zookeeper_quorum=""
for host in config['clusterHostInfo']['zookeeper_hosts']:
  zookeeper_quorum += host + ":"+str(zookeeper_port)
  index += 1
  if index < len(config['clusterHostInfo']['zookeeper_hosts']):
    zookeeper_quorum += ","

