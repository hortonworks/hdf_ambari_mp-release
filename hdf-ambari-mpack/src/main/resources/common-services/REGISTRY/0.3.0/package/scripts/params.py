#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
from resource_management.libraries.functions import format
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.get_stack_version import get_stack_version
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import get_kinit_path
from resource_management.core.logger import Logger
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = Script.get_stack_root()
stack_name = default("/hostLevelParams/stack_name", None)
if stack_name == "HDP":
  # Override HDP stack root
  stack_root = "/usr/hdf"
retryAble = default("/commandParams/command_retry_enabled", False)

# Version being upgraded/downgraded to
version = default("/commandParams/version", None)

# Version that is CURRENT.
current_version = default("/hostLevelParams/current_version", None)


stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)
upgrade_direction = default("/commandParams/upgrade_direction", None)
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

stack_support_remove_rootpath = check_stack_feature('registry_remove_rootpath', version_for_stack_feature_checks)


# When downgrading the 'version' and 'current_version' are both pointing to the downgrade-target version
# downgrade_from_version provides the source-version the downgrade is happening from
downgrade_from_version = default("/commandParams/downgrade_from_version", None)

hostname = config['hostname']

# default registry parameters
registry_home = os.path.join(stack_root, "current", "registry")
registry_bin = os.path.join(registry_home, "bin", "registry")

registry_managed_log_dir = os.path.join(registry_home, "logs")
conf_dir = os.path.join(registry_home, "conf")

limits_conf_dir = "/etc/security/limits.d"

registry_user_nofile_limit = default('/configurations/registry-env/registry_user_nofile_limit', 65536)
registry_user_nproc_limit = default('/configurations/registry-env/registry_user_nproc_limit', 65536)

registry_user = config['configurations']['registry-env']['registry_user']
registry_log_dir = config['configurations']['registry-env']['registry_log_dir']

# This is hardcoded on the registry bash process lifecycle on which we have no control over
registry_managed_pid_dir = "/var/run/registry"
streamine_managed_log_dir = "/var/log/registry"

user_group = config['configurations']['cluster-env']['user_group']
java64_home = config['hostLevelParams']['java_home']
registry_env_sh_template = config['configurations']['registry-env']['content']

if security_enabled:
  _hostname_lowercase = config['hostname'].lower()
  registry_ui_keytab_path = config['configurations']['registry-env']['registry_ui_keytab']
  _registry_ui_jaas_principal_name = config['configurations']['registry-env']['registry_ui_principal_name']
  registry_ui_jaas_principal = _registry_ui_jaas_principal_name.replace('_HOST',_hostname_lowercase)
  registry_kerberos_params = "-Djava.security.auth.login.config="+ conf_dir +"/registry_jaas.conf"
  registry_servlet_filter = config['configurations']['registry-common']['servlet.filter']
  registry_servlet_kerberos_name_rules = config['configurations']['registry-common']['kerberos.name.rules']
  #registry_servlet_token_validity = (config['configurations']['registry-common']['token.validity'])
  registry_servlet_token_validity = 36000
  
  

registry_log_dir = config['configurations']['registry-env']['registry_log_dir']
registry_log_maxbackupindex = config['configurations']['registry-log4j']['registry_log_maxbackupindex']
registry_log_maxfilesize = config['configurations']['registry-log4j']['registry_log_maxfilesize']
registry_log_template = config['configurations']['registry-log4j']['content']
registry_log_template = registry_log_template.replace('{{registry_log_dir}}', registry_log_dir)
registry_log_template = registry_log_template.replace('{{registry_log_maxbackupindex}}', registry_log_maxbackupindex)
registry_log_template = registry_log_template.replace('{{registry_log_maxfilesize}}', ("%sMB" % registry_log_maxfilesize))

# flatten registry configs
jar_storage = config['configurations']['registry-common']['jar.storage']
registry_storage_type = str(config['configurations']['registry-common']['registry.storage.type']).lower()
registry_storage_connector_connectorURI = config['configurations']['registry-common']['registry.storage.connector.connectURI']
registry_storage_connector_user = config['configurations']['registry-common']['registry.storage.connector.user']
registry_storage_connector_password = config['configurations']['registry-common']['registry.storage.connector.password']
registry_storage_query_timeout = config['configurations']['registry-common']['registry.storage.query.timeout']

jar_storage_type = config['configurations']['registry-common']['jar.storage.type']
jar_storage_hdfs_url = config['configurations']['registry-common']['jar.storage.hdfs.url']
jar_storage = config['configurations']['registry-common']['jar.storage']
jar_storage_class = "com.hortonworks.registries.common.util.LocalFileSystemStorage"
jar_remote_storage_enabled  = False


if jar_storage_type != None and jar_storage_type == "hdfs":
  jar_storage_class = "com.hortonworks.registries.common.util.HdfsFileStorage"
  jar_remote_storage_enabled = True

if registry_storage_type == "postgresql":
  registry_storage_java_class = "org.postgresql.ds.PGSimpleDataSource"
elif registry_storage_type == "oracle":
  registry_storage_java_class = "oracle.jdbc.pool.OracleDataSource"
else:
  registry_storage_java_class = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"


registry_port = config['configurations']['registry-common']['port']
registry_admin_port = config['configurations']['registry-common']['adminPort']

registry_schema_cache_size = config['configurations']['registry-common']['registry.schema.cache.size']
registry_schema_cache_expiry_interval = config['configurations']['registry-common']['registry.schema.cache.expiry.interval']


# database jar
jdk_location = config['hostLevelParams']['jdk_location']

if 'mysql' == registry_storage_type:
  jdbc_driver_jar = default("/hostLevelParams/custom_mysql_jdbc_name", None)
  if jdbc_driver_jar == None:
    Logger.error("Failed to find mysql-java-connector jar. Make sure you followed the steps to register mysql driver")
    Logger.info("Users should register the mysql java driver jar.")
    Logger.info("yum install mysql-connector-java*")
    Logger.info("sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar")

if 'oracle' == registry_storage_type:
  jdbc_driver_jar = default("/hostLevelParams/custom_oracle_jdbc_name", None)
  if jdbc_driver_jar == None:
    Logger.error("Failed to find ojdbc jar. Please download and sure you followed the steps to register oracle driver")
    Logger.info("Users should register the oracle java driver jar.")
    Logger.info("Create a symlink e.g. ln -s /usr/share/java/ojdbc6.jar /usr/share/java/ojdbc.jar")
    Logger.info("sudo ambari-server setup --jdbc-db=oracle --jdbc-driver=/usr/share/java/ojdbc.jar")

if 'mysql' == registry_storage_type or 'oracle' == registry_storage_type:
  connector_curl_source = format("{jdk_location}/{jdbc_driver_jar}")
  connector_download_dir=format("{registry_home}/libs")
  connector_bootstrap_download_dir=format("{registry_home}/bootstrap/lib")
  downloaded_custom_connector = format("{tmp_dir}/{jdbc_driver_jar}")

check_db_connection_jar_name = "DBConnectionVerification.jar"
check_db_connection_jar = format("/usr/lib/ambari-agent/{check_db_connection_jar_name}")

# bootstrap commands
jdk64_home=config['hostLevelParams']['java_home']
bootstrap_storage_command = os.path.join(registry_home, "bootstrap", "bootstrap-storage.sh")
bootstrap_storage_run_cmd = format('export JAVA_HOME={jdk64_home} ; source {conf_dir}/registry-env.sh ; {bootstrap_storage_command}')
registry_agent_dir = "/var/lib/ambari-agent/data/registry"
