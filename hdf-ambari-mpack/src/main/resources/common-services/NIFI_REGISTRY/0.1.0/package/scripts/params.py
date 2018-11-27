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

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core import sudo
import sys, os, glob, socket, re
from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.version_select_util import *
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set

import config_utils

# server configurations
config = Script.get_config()
stack_root = Script.get_stack_root()
tmp_dir = Script.get_tmp_dir()
stack_name = default("/clusterLevelParams/stack_name", None)
stack_version_buildnum = default("/commandParams/version", None)
if stack_name == "HDP":
    # Override HDP stack root
    stack_root = "/usr/hdf"
    # Override HDP stack version
    stack_version_buildnum = get_component_version_with_stack_selector("/usr/bin/hdf-select", "nifi-registry")
elif not stack_version_buildnum and stack_name:
    stack_version_buildnum = get_component_version_from_symlink(stack_name, "nifi-registry")

service_name = 'nifi-registry'
version_for_stack_feature_checks = get_stack_feature_version(config)

script_dir = os.path.dirname(__file__)
toolkit_files_dir = os.path.realpath(os.path.join(os.path.dirname(script_dir), 'files'))
toolkit_tmp_dir = tmp_dir

# Version being upgraded/downgraded to
version = default("/commandParams/version", None)
#upgrade direction
upgrade_direction = default("/commandParams/upgrade_direction", None)

nifi_registry_install_dir = os.path.join(stack_root, "current", "nifi-registry")

# params from nifi-registry-ambari-config
nifi_registry_initial_mem = config['configurations']['nifi-registry-ambari-config']['nifi.registry.initial_mem']
nifi_registry_max_mem = config['configurations']['nifi-registry-ambari-config']['nifi.registry.max_mem']

# note: nifi.registry.port and nifi.registry.port.ssl must be defined in same xml file for quicklinks to work
nifi_registry_port = config['configurations']['nifi-registry-ambari-config']['nifi.registry.port']
nifi_registry_ssl_port = config['configurations']['nifi-registry-ambari-config']['nifi.registry.port.ssl']

nifi_registry_internal_dir=config['configurations']['nifi-registry-ambari-config']['nifi.registry.internal.dir']
nifi_registry_internal_config_dir=config['configurations']['nifi-registry-ambari-config']['nifi.registry.internal.config.dir']
nifi_registry_internal_config_dir = nifi_registry_internal_config_dir.replace('{nifi_registry_internal_dir}', nifi_registry_internal_dir)

nifi_registry_config_dir= config['configurations']['nifi-registry-ambari-config']['nifi.registry.config.dir']
nifi_registry_config_dir = nifi_registry_config_dir.replace('{nifi_registry_install_dir}', nifi_registry_install_dir)

nifi_registry_database_dir=config['configurations']['nifi-registry-ambari-config']['nifi.registry.database.dir']
nifi_registry_database_dir = nifi_registry_database_dir.replace('{nifi_registry_internal_dir}', nifi_registry_internal_dir)

# password for encrypted config
nifi_registry_security_encrypt_configuration_password = config['configurations']['nifi-registry-ambari-config']['nifi.registry.security.encrypt.configuration.password']

master_configs = config['clusterHostInfo']
nifi_registry_master_hosts = master_configs['nifi_registry_master_hosts']

#nifi registry bootstrap file location
nifi_registry_bootstrap_file = nifi_registry_config_dir + '/bootstrap.conf'

nifi_registry_dir=nifi_registry_install_dir
bin_dir = os.path.join(*[nifi_registry_dir,'bin'])
lib_dir = os.path.join(*[nifi_registry_dir,'lib'])
docs_dir = os.path.join(*[nifi_registry_dir,'docs'])

nifi_ca_host = None
if 'nifi_ca_hosts' in master_configs:
    nifi_ca_hosts = master_configs['nifi_ca_hosts']
    if len(nifi_ca_hosts) > 0:
        nifi_ca_host = nifi_ca_hosts[0]

# params from nifi-registry-ambari-ssl-config
nifi_registry_ssl_enabled = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.ssl.isenabled']
nifi_registry_keystore = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keystore']
nifi_registry_keystoreType = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keystoreType']
nifi_registry_keystorePasswd = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keystorePasswd']
nifi_registry_keyPasswd = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keyPasswd']
nifi_registry_truststore = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.truststore']
nifi_registry_truststoreType = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.truststoreType']
nifi_registry_truststorePasswd = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.truststorePasswd']
nifi_registry_needClientAuth = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.needClientAuth']
nifi_registry_initial_admin_id = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.initial.admin.identity']
nifi_registry_ssl_config_content = config['configurations']['nifi-registry-ambari-ssl-config']['content']

#default keystore/truststore type if empty
nifi_registry_keystoreType = 'jks' if len(nifi_registry_keystoreType) == 0 else nifi_registry_keystoreType
nifi_registry_truststoreType = 'jks' if len(nifi_registry_truststoreType) == 0 else nifi_registry_truststoreType

#property that is set to hostname regardless of whether SSL enabled
nifi_registry_host = socket.getfqdn()

nifi_registry_truststore = nifi_registry_truststore.replace('{nifi_registry_ssl_host}',nifi_registry_host)
nifi_registry_keystore = nifi_registry_keystore.replace('{nifi_registry_ssl_host}',nifi_registry_host)

#populate properties whose values depend on whether SSL enabled
nifi_registry_keystore = nifi_registry_keystore.replace('{{nifi_registry_config_dir}}',nifi_registry_config_dir)
nifi_registry_truststore = nifi_registry_truststore.replace('{{nifi_registry_config_dir}}',nifi_registry_config_dir)

if nifi_registry_ssl_enabled:
    nifi_registry_ssl_host = nifi_registry_host
    nifi_registry_port = ""
else:
    nifi_registry_nonssl_host = nifi_registry_host
    nifi_registry_ssl_port = ""

# wrap this in a check to see if we have a ca host b/c otherwise nifi-ambari-ssl-config won't exist
# we use nifi-ambari-ssl-config to get the values for the CA so that they aren't duplicated in nifi-registry
if nifi_ca_host:
    nifi_ca_parent_config = config['configurations']['nifi-ambari-ssl-config']
    nifi_use_ca = nifi_ca_parent_config['nifi.toolkit.tls.token']
    nifi_toolkit_tls_token = nifi_ca_parent_config['nifi.toolkit.tls.token']
    nifi_toolkit_tls_helper_days = nifi_ca_parent_config['nifi.toolkit.tls.helper.days']
    nifi_toolkit_tls_port = nifi_ca_parent_config['nifi.toolkit.tls.port']
    nifi_toolkit_dn_prefix = nifi_ca_parent_config['nifi.toolkit.dn.prefix']
    nifi_toolkit_dn_suffix = nifi_ca_parent_config['nifi.toolkit.dn.suffix']

    nifi_ca_log_file_stdout = config['configurations']['nifi-registry-env']['nifi_registry_log_dir'] + '/nifi-ca.stdout'
    nifi_ca_log_file_stderr = config['configurations']['nifi-registry-env']['nifi_registry_log_dir'] + '/nifi-ca.stderr'

    stack_support_tls_toolkit_san = check_stack_feature('tls_toolkit_san', version_for_stack_feature_checks)

    nifi_ca_client_config = {
        "days" : int(nifi_toolkit_tls_helper_days),
        "keyStore" : nifi_registry_keystore,
        "keyStoreType" : nifi_registry_keystoreType,
        "keyStorePassword" : nifi_registry_keystorePasswd,
        "keyPassword" : nifi_registry_keyPasswd,
        "token" : nifi_toolkit_tls_token,
        "dn" : nifi_toolkit_dn_prefix + nifi_registry_host + nifi_toolkit_dn_suffix,
        "port" : int(nifi_toolkit_tls_port),
        "caHostname" : nifi_ca_host,
        "trustStore" : nifi_registry_truststore,
        "trustStoreType" : nifi_registry_truststoreType,
        "trustStorePassword": nifi_registry_truststorePasswd
    }

    if stack_support_tls_toolkit_san:
        nifi_ca_client_config["domainAlternativeNames"] = nifi_registry_host

# this comes from the registry side since regenerate is a separate operation for nifi and nifi-registry
nifi_toolkit_tls_regenerate = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.toolkit.tls.regenerate']

# params from nifi-registry-env
nifi_registry_user = config['configurations']['nifi-registry-env']['nifi_registry_user']
nifi_registry_group = config['configurations']['nifi-registry-env']['nifi_registry_group']

nifi_registry_log_dir = config['configurations']['nifi-registry-env']['nifi_registry_log_dir']
nifi_registry_log_file = os.path.join(nifi_registry_log_dir,'nifi-registry-setup.log')

# params from nifi-registry-boostrap
nifi_registry_env_content = config_utils.merge_env(config['configurations']['nifi-registry-env'])

# params from nifi-registry-logback
nifi_registry_logback_content = config['configurations']['nifi-registry-logback-env']['content']

# params from nifi-registry-properties-env
nifi_registry_master_properties_content = config['configurations']['nifi-registry-master-properties-env']['content']
nifi_registry_properties = config['configurations']['nifi-registry-properties'].copy()

#kerberos params
nifi_registry_kerberos_authentication_expiration = config['configurations']['nifi-registry-properties']['nifi.registry.kerberos.spnego.authentication.expiration']
nifi_registry_kerberos_realm = default("/configurations/kerberos-env/realm", None)

# params from nifi-registry-authorizers-env
nifi_registry_authorizers_content = config['configurations']['nifi-registry-authorizers-env']['content']
nifi_registry_authorizers_dict = config['configurations']['nifi-registry-authorizers-env']
# params from nifi-registry-identity-providers-env
nifi_registry_identity_providers_content = config['configurations']['nifi-registry-identity-providers-env']['content']
nifi_registry_identity_providers_dict = config['configurations']['nifi-registry-identity-providers-env']
# params from nifi-registry-providers-env
nifi_registry_providers_content = config['configurations']['nifi-registry-providers-env']['content']
nifi_registry_providers_dict = config['configurations']['nifi-registry-providers-env']
# params from nifi-registry-boostrap
nifi_registry_boostrap_content = config_utils.merge_env(config['configurations']['nifi-registry-bootstrap-env'])

# params from nifi-toolkit-env
nifi_toolkit_java_options = config['configurations']['nifi-toolkit-env']['nifi_toolkit_java_options'] if 'nifi-toolkit-env' in config['configurations'] else '-Xms128m -Xmx256m'

#autodetect jdk home
jdk64_home=config['ambariLevelParams']['java_home']

nifi_registry_authorizer = 'managed-authorizer'

java_home = config['ambariLevelParams']['java_home']
security_enabled = config['configurations']['cluster-env']['security_enabled']
smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))

stack_support_toolkit_update = check_stack_feature('toolkit_config_update', version_for_stack_feature_checks)
stack_support_admin_toolkit = check_stack_feature('admin_toolkit_support', version_for_stack_feature_checks)
stack_support_nifi_toolkit_package = check_stack_feature('nifi_toolkit_package', version_for_stack_feature_checks)
#some released HDP stacks will not have this stack feature, manually check
if not stack_support_nifi_toolkit_package and stack_name == "HDP":
    marker_script = os.path.join(stack_root, "current/nifi-toolkit/bin/tls-toolkit.sh")
    if sudo.path_isfile(marker_script):
        stack_support_nifi_toolkit_package = True

if security_enabled:
    _hostname_lowercase = nifi_registry_host.lower()
    nifi_registry_properties['nifi.registry.kerberos.spnego.principal'] = nifi_registry_properties['nifi.registry.kerberos.spnego.principal'].replace('_HOST',_hostname_lowercase)
