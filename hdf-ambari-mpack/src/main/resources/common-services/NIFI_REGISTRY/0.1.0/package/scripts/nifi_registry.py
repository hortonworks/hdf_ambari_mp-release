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

import sys, nifi_toolkit_util, os, pwd, grp, signal, time, glob, socket
from resource_management import *
from resource_management.core import sudo
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.constants import Direction
from resource_management.core.exceptions import Fail

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
    def get_component_name(self):
        stack_name = default("/hostLevelParams/stack_name", None)
        if stack_name == "HDP":
            return None
        return "nifi-registry"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        Logger.info("Executing Stack Upgrade pre-restart")
        import params
        env.set_params(params)

        if params.version and check_stack_feature(StackFeature.ROLLING_UPGRADE, format_stack_version(params.version)):
            stack_select.select("nifi-registry", params.version)
        if params.version and check_stack_feature(StackFeature.CONFIG_VERSIONING, params.version):
            conf_select.select(params.stack_name, "nifi-registry", params.version)

    def post_upgrade_restart(self, env, upgrade_type=None):
        pass

    def install(self, env):
        import params

        self.install_packages(env)

        # params.nifi_registry_dir,
        Directory([params.nifi_registry_log_dir],
                  owner=params.nifi_registry_user,
                  group=params.nifi_registry_group,
                  create_parents=True,
                  recursive_ownership=True,
                  cd_access='a'
                  )

        nifi_toolkit_util.copy_toolkit_scripts(params.toolkit_files_dir, params.toolkit_tmp_dir, params.nifi_registry_user, params.nifi_registry_group, upgrade_type=None)
        Execute('touch ' +  params.nifi_registry_log_file, user=params.nifi_registry_user)


    def configure(self, env, isInstall=False, is_starting = False):
        import params
        import status_params
        env.set_params(params)
        env.set_params(status_params)

        #create the log, pid, conf dirs if not already present
        nifi_registry_dirs = [status_params.nifi_registry_pid_dir,
                              params.nifi_registry_internal_dir,
                              params.nifi_registry_internal_config_dir,
                              params.nifi_registry_database_dir,
                              params.nifi_registry_config_dir,
                              params.bin_dir,
                              params.lib_dir,
                              params.docs_dir]

        Directory(nifi_registry_dirs,
                  owner=params.nifi_registry_user,
                  group=params.nifi_registry_group,
                  create_parents=True,
                  recursive_ownership=True,
                  cd_access='a')

        config_version_file = format("{params.nifi_registry_config_dir}/config_version")

        if params.nifi_ca_host and params.nifi_registry_ssl_enabled:
            params.nifi_registry_properties = nifi_toolkit_util.setup_keystore_truststore(is_starting, params, config_version_file)
        elif params.nifi_ca_host and not params.nifi_registry_ssl_enabled:
            params.nifi_registry_properties = nifi_toolkit_util.cleanup_toolkit_client_files(params, config_version_file)

        #write configurations
        self.write_configurations(params)

        nifi_toolkit_util.encrypt_sensitive_properties(params.config, config_version_file, params.nifi_registry_config_dir, params.jdk64_home,
                                                        params.nifi_toolkit_java_options, params.nifi_registry_user,
                                                        params.nifi_registry_group, params.nifi_registry_security_encrypt_configuration_password,
                                                        is_starting, params.toolkit_tmp_dir)

    def stop(self, env, upgrade_type=None):
        import params
        import status_params
        env.set_params(params)
        env.set_params(status_params)

        # this method will be called during an upgrade before start/configure get to setup all the permissions so we need to do it here too
        Directory([params.bin_dir],
                  owner=params.nifi_registry_user,
                  group=params.nifi_registry_group,
                  create_parents=True,
                  recursive_ownership=True,
                  cd_access='a'
                  )

        env_content=InlineTemplate(params.nifi_registry_env_content)
        File(format("{params.bin_dir}/nifi-registry-env.sh"), content=env_content, owner=params.nifi_registry_user, group=params.nifi_registry_group, mode=0755)

        Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi-registry.sh stop >> ' + params.nifi_registry_log_file, user=params.nifi_registry_user)
        if os.path.isfile(status_params.nifi_registry_pid_file):
            sudo.unlink(status_params.nifi_registry_pid_file)

    def start(self, env, upgrade_type=None):
        import params
        import status_params

        nifi_toolkit_util.copy_toolkit_scripts(params.toolkit_files_dir, params.toolkit_tmp_dir, params.nifi_registry_user, params.nifi_registry_group, upgrade_type=None)
        self.configure(env, is_starting = True)
        #setup_ranger_nifi(upgrade_type=None)

        Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi-registry.sh start >> ' + params.nifi_registry_log_file, user=params.nifi_registry_user)
        #If nifi pid file not created yet, wait a bit
        if not os.path.isfile(status_params.nifi_registry_pid_dir+'/nifi-registry.pid'):
            Execute ('sleep 5')

    def status(self, env):
        import status_params
        check_process_status(status_params.nifi_registry_pid_file)

    def setup_tls_toolkit_upgrade(self,env):
        import params
        env.set_params(params)

        upgrade_stack = stack_select._get_upgrade_stack()
        if upgrade_stack is None:
            raise Fail('Unable to determine the stack and stack version')

        if params.upgrade_direction == Direction.UPGRADE and params.nifi_registry_ssl_enabled and params.nifi_ca_host:
            version_file = params.nifi_registry_config_dir + '/config_version'
            client_json_file = params.nifi_registry_config_dir+ '/nifi-certificate-authority-client.json'

            if not sudo.path_isfile(version_file):
                Logger.info(format('Create config version file if it does not exist'))
                nifi_toolkit_util.save_config_version(params.config, version_file, 'ssl', params.nifi_registry_user, params.nifi_registry_group)

            if sudo.path_isfile(client_json_file):
                Logger.info(format('Remove client json file'))
                sudo.unlink(client_json_file)

    def write_configurations(self, params):

        #write out nifi.properties
        PropertiesFile(params.nifi_registry_config_dir + '/nifi-registry.properties',
                       properties = params.nifi_registry_properties,
                       mode = 0600,
                       owner = params.nifi_registry_user,
                       group = params.nifi_registry_group)

        #write out boostrap.conf
        bootstrap_content=InlineTemplate(params.nifi_registry_boostrap_content)

        File(format("{params.nifi_registry_bootstrap_file}"),
             content=bootstrap_content,
             owner=params.nifi_registry_user,
             group=params.nifi_registry_group,
             mode=0600)

        #write out logback.xml
        logback_content=InlineTemplate(params.nifi_registry_logback_content)

        File(format("{params.nifi_registry_config_dir}/logback.xml"),
             content=logback_content,
             owner=params.nifi_registry_user,
             group=params.nifi_registry_group,
             mode=0400)

        #write out authorizers file
        authorizers_content=InlineTemplate(params.nifi_registry_authorizers_content)

        File(format("{params.nifi_registry_config_dir}/authorizers.xml"),
             content=authorizers_content,
             owner=params.nifi_registry_user,
             group=params.nifi_registry_group,
             mode=0600)

        #write out identity-providers.xml
        identity_providers_content=InlineTemplate(params.nifi_registry_identity_providers_content)

        File(format("{params.nifi_registry_config_dir}/identity-providers.xml"),
             content=identity_providers_content,
             owner=params.nifi_registry_user,
             group=params.nifi_registry_group,
             mode=0600)

        #write out providers file
        providers_content=InlineTemplate(params.nifi_registry_providers_content)

        File(format("{params.nifi_registry_config_dir}/providers.xml"),
             content=providers_content,
             owner=params.nifi_registry_user,
             group=params.nifi_registry_group,
             mode=0400)

        #write out nifi-env in bin as 0755 (see BUG-61769)
        env_content=InlineTemplate(params.nifi_registry_env_content)

        File(format("{params.bin_dir}/nifi-registry-env.sh"),
             content=env_content,
             owner=params.nifi_registry_user,
             group=params.nifi_registry_group,
             mode=0755)

        #if security is enabled for kerberos create the nifi_jaas.conf file
        #if params.security_enabled and params.stack_support_nifi_jaas:
        #    File(params.nifi_jaas_conf, content=InlineTemplate(params.nifi_jaas_conf_template), owner=params.nifi_user, group=params.nifi_group, mode=0400)


if __name__ == "__main__":
    Master().execute()
