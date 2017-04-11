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
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.functions.setup_ranger_plugin_xml import get_audit_configs
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = Script.get_stack_root()
stack_name = default("/hostLevelParams/stack_name", None)
retryAble = default("/commandParams/command_retry_enabled", False)

# Version being upgraded/downgraded to
version = default("/commandParams/version", None)

# Version that is CURRENT.
current_version = default("/hostLevelParams/current_version", None)


stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)
upgrade_direction = default("/commandParams/upgrade_direction", None)

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

stack_supports_ranger_kerberos = check_stack_feature(StackFeature.RANGER_KERBEROS_SUPPORT, version_for_stack_feature_checks)
stack_supports_ranger_audit_db = check_stack_feature(StackFeature.RANGER_AUDIT_DB_SUPPORT, version_for_stack_feature_checks)
stack_supports_core_site_for_ranger_plugin = check_stack_feature(StackFeature.CORE_SITE_FOR_RANGER_PLUGINS_SUPPORT, version_for_stack_feature_checks)

# When downgrading the 'version' and 'current_version' are both pointing to the downgrade-target version
# downgrade_from_version provides the source-version the downgrade is happening from
downgrade_from_version = default("/commandParams/downgrade_from_version", None)

hostname = config['hostname']

# default streamline parameters
streamline_home = os.path.join(stack_root, "current", "streamline")
streamline_bin = os.path.join(streamline_home, "bin", "streamline")

streamline_managed_log_dir = os.path.join(streamline_home, "logs")
conf_dir = os.path.join(streamline_home, "conf")

limits_conf_dir = "/etc/security/limits.d"

streamline_user_nofile_limit = default('/configurations/streamline-env/streamline_user_nofile_limit', 65536)
streamline_user_nproc_limit = default('/configurations/streamline-env/streamline_user_nproc_limit', 65536)

streamline_user = config['configurations']['streamline-env']['streamline_user']
streamline_log_dir = config['configurations']['streamline-env']['streamline_log_dir']

# This is hardcoded on the streamline bash process lifecycle on which we have no control over
streamline_managed_pid_dir = "/var/run/streamline"
streamine_managed_log_dir = "/var/log/streamline"

user_group = config['configurations']['cluster-env']['user_group']
java64_home = config['hostLevelParams']['java_home']
streamline_env_sh_template = config['configurations']['streamline-env']['content']



# flatten streamline configs

storm_client_home = config['configurations']['streamline-common']['storm.client.home']
registry_url = config['configurations']['streamline-common']['registry.url']
maven_repo_url = config['configurations']['streamline-common']['maven.repo.url']
jar_storage = config['configurations']['streamline-common']['jar.storage']
streamline_dashboard_url = config['configurations']['streamline-common']['streamline.dashboard.url']

streamline_storage_type = config['configurations']['streamline-common']['streamline.storage.type']
streamline_storage_connector_connectorURI = config['configurations']['streamline-common']['streamline.storage.connector.connectURI']
streamline_storage_connector_user = config['configurations']['streamline-common']['streamline.storage.connector.user']
streamline_storage_connector_password = config['configurations']['streamline-common']['streamline.storage.connector.password']
streamline_storage_query_timeout = config['configurations']['streamline-common']['streamline.storage.query.timeout']

streamline_port = config['configurations']['streamline-common']['port']
streamline_admin_port = config['configurations']['streamline-common']['adminPort']

streamline_catalog_root_url = 'http://{0}:{1}/api/v1/catalog'.format(hostname,streamline_port)

# mysql jar
jdk_location = config['hostLevelParams']['jdk_location']
if 'mysql' == streamline_storage_type:
  jdbc_driver_jar = default("/hostLevelParams/custom_mysql_jdbc_name", None)
  if jdbc_driver_jar == None:
    Logger.error("Failed to find mysql-java-connector jar. Make sure you followed the steps to register mysql driver")
    Logger.info("Users should register the mysql java driver jar.")
    Logger.info("yum install mysql-connector-java*")
    Logger.info("sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar")
    raise
  connector_curl_source = format("{jdk_location}/{jdbc_driver_jar}")
  connector_download_dir=format("{streamline_home}/libs")
  connector_bootstrap_download_dir=format("{streamline_home}/bootstrap/lib")
  downloaded_custom_connector = format("{tmp_dir}/{jdbc_driver_jar}")
  

check_db_connection_jar_name = "DBConnectionVerification.jar"
check_db_connection_jar = format("/usr/lib/ambari-agent/{check_db_connection_jar_name}")

# bootstrap commands

bootstrap_storage_command = os.path.join(streamline_home, "bootstrap", "bootstrap-storage.sh")
bootstrap_storage_run_cmd = format('source {conf_dir}/streamline-env.sh ; {bootstrap_storage_command}')

bootstrap_command = os.path.join(streamline_home, "bootstrap", "bootstrap.sh")
bootstrap_run_cmd = format('source {conf_dir}/streamline-env.sh ; {bootstrap_command}')

bootstrap_storage_file = "/var/lib/ambari-agent/data/streamline/bootstrap_storage_done"
bootstrap_file = "/var/lib/ambari-agent/data/streamline/bootstrap_done"
streamline_agent_dir = "/var/lib/ambari-agent/data/streamline"
