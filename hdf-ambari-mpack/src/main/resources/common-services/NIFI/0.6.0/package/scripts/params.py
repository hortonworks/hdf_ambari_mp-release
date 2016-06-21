#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script
import sys, os, glob
from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set
    
# server configurations
config = Script.get_config()

nifi_install_dir = '/usr/hdf/current/nifi'

# params from nifi-ambari-config
nifi_initial_mem = config['configurations']['nifi-ambari-config']['nifi.initial_mem']
nifi_max_mem = config['configurations']['nifi-ambari-config']['nifi.max_mem']
nifi_ambari_reporting_frequency = config['configurations']['nifi-ambari-config']['nifi.ambari_reporting_frequency']

nifi_node_port = config['configurations']['nifi-ambari-config']['nifi.node.port']
nifi_node_protocol_port = config['configurations']['nifi-ambari-config']['nifi.node.protocol.port']

nifi_znode = config['configurations']['nifi-ambari-config']['nifi.nifi_znode']

  
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

# params from nifi-authorizers
nifi_authorizer_content = config['configurations']['nifi-authorizers']['content']

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


#setup ranger configuration

retryAble = default("/commandParams/command_retry_enabled", False)
version = default("/commandParams/version", None)
namenode_hosts = default("/clusterHostInfo/namenode_host", None)

if type(namenode_hosts) is list:
  namenode_host = namenode_hosts[0]
else:
  namenode_host = namenode_hosts

has_namenode = not namenode_host == None


nifi_authorizer = 'file-provider'

nifi_host_name_in_cluster = config['hostname']
nifi_host_port = config['configurations']['gateway-site']['gateway.port']
java_home = config['hostLevelParams']['java_home']
security_enabled = config['configurations']['cluster-env']['security_enabled']
smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))

if security_enabled:
  nifi_keytab_path = config['configurations']['nifi-env']['nifi_keytab_path']
  _hostname_lowercase = config['hostname'].lower()
  nifi_principal_name = config['configurations']['nifi-env']['nifi_principal_name'].replace('_HOST',_hostname_lowercase)

# ranger host
# E.g., 2.3
stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)
stack_supports_ranger_kerberos = stack_version_formatted and check_stack_feature(StackFeature.RANGER_KERBEROS_SUPPORT, stack_version_formatted)
stack_supports_ranger_audit_db = stack_version_formatted and check_stack_feature(StackFeature.RANGER_AUDIT_DB_SUPPORT, stack_version_formatted)

ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
has_ranger_admin = not len(ranger_admin_hosts) == 0
xml_configurations_supported = config['configurations']['ranger-env']['xml_configurations_supported']

ambari_server_hostname = config['clusterHostInfo']['ambari_server_host'][0]

# ranger nifi properties
policymgr_mgr_url = config['configurations']['admin-properties']['policymgr_external_url']

if 'admin-properties' in config['configurations'] and 'policymgr_external_url' in config['configurations']['admin-properties'] and policymgr_mgr_url.endswith('/'):
  policymgr_mgr_url = policymgr_mgr_url.rstrip('/')

xa_audit_db_name = config['configurations']['admin-properties']['audit_db_name']
xa_audit_db_user = config['configurations']['admin-properties']['audit_db_user']
xa_db_host = config['configurations']['admin-properties']['db_host']
repo_name = str(config['clusterName']) + '_nifi'

common_name_for_certificate = config['configurations']['ranger-nifi-plugin-properties']['common.name.for.certificate']
repo_config_username = config['configurations']['ranger-nifi-plugin-properties']['REPOSITORY_CONFIG_USERNAME']

ranger_env = config['configurations']['ranger-env']
ranger_plugin_properties = config['configurations']['ranger-nifi-plugin-properties']
policy_user = config['configurations']['ranger-nifi-plugin-properties']['policy_user']

#For curl command in ranger plugin to get db connector
jdk_location = config['hostLevelParams']['jdk_location']
java_share_dir = '/usr/share/java'

if has_ranger_admin:
  enable_ranger_nifi = (config['configurations']['ranger-nifi-plugin-properties']['ranger-nifi-plugin-enabled'].lower() == 'yes')
  xa_audit_db_password = unicode(config['configurations']['admin-properties']['audit_db_password']) if stack_supports_ranger_audit_db else None
  repo_config_password = unicode(config['configurations']['ranger-nifi-plugin-properties']['REPOSITORY_CONFIG_PASSWORD'])
  xa_audit_db_flavor = (config['configurations']['admin-properties']['DB_FLAVOR']).lower()
  previous_jdbc_jar_name= None

  if stack_supports_ranger_audit_db:
    if xa_audit_db_flavor == 'mysql':
      jdbc_jar_name = default("/hostLevelParams/custom_mysql_jdbc_name", None)
      previous_jdbc_jar_name = default("/hostLevelParams/previous_custom_mysql_jdbc_name", None)
      audit_jdbc_url = format('jdbc:mysql://{xa_db_host}/{xa_audit_db_name}')
      jdbc_driver = "com.mysql.jdbc.Driver"
    elif xa_audit_db_flavor == 'oracle':
      jdbc_jar_name = default("/hostLevelParams/custom_oracle_jdbc_name", None)
      previous_jdbc_jar_name = default("/hostLevelParams/previous_custom_oracle_jdbc_name", None)
      colon_count = xa_db_host.count(':')
      if colon_count == 2 or colon_count == 0:
        audit_jdbc_url = format('jdbc:oracle:thin:@{xa_db_host}')
      else:
        audit_jdbc_url = format('jdbc:oracle:thin:@//{xa_db_host}')
      jdbc_driver = "oracle.jdbc.OracleDriver"
    elif xa_audit_db_flavor == 'postgres':
      jdbc_jar_name = default("/hostLevelParams/custom_postgres_jdbc_name", None)
      previous_jdbc_jar_name = default("/hostLevelParams/previous_custom_postgres_jdbc_name", None)
      audit_jdbc_url = format('jdbc:postgresql://{xa_db_host}/{xa_audit_db_name}')
      jdbc_driver = "org.postgresql.Driver"
    elif xa_audit_db_flavor == 'mssql':
      jdbc_jar_name = default("/hostLevelParams/custom_mssql_jdbc_name", None)
      previous_jdbc_jar_name = default("/hostLevelParams/previous_custom_mssql_jdbc_name", None)
      audit_jdbc_url = format('jdbc:sqlserver://{xa_db_host};databaseName={xa_audit_db_name}')
      jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    elif xa_audit_db_flavor == 'sqla':
      jdbc_jar_name = default("/hostLevelParams/custom_sqlanywhere_jdbc_name", None)
      previous_jdbc_jar_name = default("/hostLevelParams/previous_custom_sqlanywhere_jdbc_name", None)
      audit_jdbc_url = format('jdbc:sqlanywhere:database={xa_audit_db_name};host={xa_db_host}')
      jdbc_driver = "sap.jdbc4.sqlanywhere.IDriver"

  downloaded_custom_connector = format("{tmp_dir}/{jdbc_jar_name}") if stack_supports_ranger_audit_db else None
  driver_curl_source = format("{jdk_location}/{jdbc_jar_name}") if stack_supports_ranger_audit_db else None

  driver_curl_target = format("{stack_root}/current/nifi/ext/{jdbc_jar_name}") if stack_supports_ranger_audit_db else None
  previous_jdbc_jar = format("{stack_root}/current/nifi/ext/{previous_jdbc_jar_name}") if stack_supports_ranger_audit_db else None
  sql_connector_jar = ''

  nifi_ranger_plugin_config = {
    'username': repo_config_username,
    'password': repo_config_password,
    'nifi.url': format("https://{nifi_host_name}:{nifi_host_port}/gateway/admin/api/v1/topologies"),
    'commonNameForCertificate': common_name_for_certificate
  }

  nifi_ranger_plugin_repo = {
    'isActive': 'true',
    'config': json.dumps(nifi_ranger_plugin_config),
    'description': 'nifi repo',
    'name': repo_name,
    'repositoryType': 'nifi',
    'assetType': '5',
  }

  if stack_supports_ranger_kerberos and security_enabled:
    nifi_ranger_plugin_config['policy.download.auth.users'] = nifi_user
    nifi_ranger_plugin_config['tag.download.auth.users'] = nifi_user

  if stack_supports_ranger_kerberos:
    nifi_ranger_plugin_config['ambari.service.check.user'] = policy_user

    nifi_ranger_plugin_repo = {
      'isEnabled': 'true',
      'configs': nifi_ranger_plugin_config,
      'description': 'nifi repo',
      'name': repo_name,
      'type': 'nifi'
    }

  xa_audit_db_is_enabled = False
  ranger_audit_solr_urls = config['configurations']['ranger-admin-site']['ranger.audit.solr.urls']

  if xml_configurations_supported and stack_supports_ranger_audit_db:
    xa_audit_db_is_enabled = config['configurations']['ranger-nifi-audit']['xasecure.audit.destination.db']

  xa_audit_hdfs_is_enabled = config['configurations']['ranger-nifi-audit']['xasecure.audit.destination.hdfs'] if xml_configurations_supported else None
  ssl_keystore_password = unicode(config['configurations']['ranger-nifi-policymgr-ssl']['xasecure.policymgr.clientssl.keystore.password']) if xml_configurations_supported else None
  ssl_truststore_password = unicode(config['configurations']['ranger-nifi-policymgr-ssl']['xasecure.policymgr.clientssl.truststore.password']) if xml_configurations_supported else None
  credential_file = format('/etc/ranger/{repo_name}/cred.jceks') if xml_configurations_supported else None

  #For SQLA explicitly disable audit to DB for Ranger
  if xa_audit_db_flavor == 'sqla':
    xa_audit_db_is_enabled = False

  nifi_authorizer = 'file-provider'

hdfs_user = config['configurations']['hadoop-env']['hdfs_user'] if has_namenode else None
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab'] if has_namenode else None
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name'] if has_namenode else None
hdfs_site = config['configurations']['hdfs-site'] if has_namenode else None
default_fs = config['configurations']['core-site']['fs.defaultFS'] if has_namenode else None
hadoop_bin_dir = stack_select.get_hadoop_dir("bin") if has_namenode else None
hadoop_conf_dir = conf_select.get_hadoop_conf_dir() if has_namenode else None

import functools
#create partial functions with common arguments for every HdfsResource call
#to create/delete hdfs directory/file/copyfromlocal we need to call params.HdfsResource in code
HdfsResource = functools.partial(
  HdfsResource,
  user=hdfs_user,
  hdfs_resource_ignore_file = "/var/lib/ambari-agent/data/.hdfs_resource_ignore",
  security_enabled = security_enabled,
  keytab = hdfs_user_keytab,
  kinit_path_local = kinit_path_local,
  hadoop_bin_dir = hadoop_bin_dir,
  hadoop_conf_dir = hadoop_conf_dir,
  principal_name = hdfs_principal_name,
  hdfs_site = hdfs_site,
  default_fs = default_fs,
  immutable_paths = get_not_managed_resources()
)