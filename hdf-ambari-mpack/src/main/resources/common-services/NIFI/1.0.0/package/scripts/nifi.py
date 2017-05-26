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

import sys, nifi_toolkit_util, os, pwd, grp, signal, time, glob, socket, json
from resource_management.core import sudo
from resource_management import *
from subprocess import call
from setup_ranger_nifi import setup_ranger_nifi
from resource_management.core.utils import PasswordString
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.constants import Direction
from resource_management.libraries.resources.template_config import TemplateConfig
from resource_management.core.exceptions import Fail

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
  def get_component_name(self):
    stack_name = default("/hostLevelParams/stack_name", None)
    if stack_name == "HDP":
      return None
    return "nifi"

  def pre_upgrade_restart(self, env, upgrade_type=None):
    Logger.info("Executing Stack Upgrade pre-restart")
    import params
    env.set_params(params)
    
    if params.version and check_stack_feature(StackFeature.ROLLING_UPGRADE, format_stack_version(params.version)):
      stack_select.select("nifi", params.version)
    if params.version and check_stack_feature(StackFeature.CONFIG_VERSIONING, params.version):
      conf_select.select(params.stack_name, "nifi", params.version)

  def post_upgrade_restart(self, env, upgrade_type=None):
    pass

  def install(self, env):
    import params

    self.install_packages(env)

    Directory([params.nifi_node_dir],
            owner=params.nifi_user,
            group=params.nifi_group,
            create_parents=True,
            recursive_ownership=True
    )

    #update the configs specified by user
    self.configure(env, True)
    Execute('touch ' +  params.nifi_node_log_file, user=params.nifi_user)


  def configure(self, env, isInstall=False, is_starting = False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)

    #create the log, pid, conf dirs if not already present
    nifi_dirs = [status_params.nifi_pid_dir, params.nifi_node_log_dir, params.nifi_internal_dir, params.nifi_database_dir, params.nifi_flowfile_repo_dir, params.nifi_provenance_repo_dir_default, params.nifi_config_dir, params.nifi_flow_config_dir, params.nifi_state_dir, params.lib_dir]
    nifi_dirs.extend(params.nifi_content_repo_dirs)
    Directory(nifi_dirs, owner=params.nifi_user, group=params.nifi_group, create_parents=True, recursive_ownership=True)

    # On some OS this folder may not exist, so we will create it before pushing files there
    Directory(params.limits_conf_dir,
              create_parents = True,
              owner='root',
              group='root'
    )

    File(os.path.join(params.limits_conf_dir, 'nifi.conf'),
         owner='root',
         group='root',
         mode=0644,
         content=Template("nifi.conf.j2")
    )

    config_version_file = format("{params.nifi_config_dir}/config_version")

    if params.nifi_ca_host and params.nifi_ssl_enabled:
      params.nifi_properties = self.setup_keystore_truststore(is_starting, params, config_version_file)
    elif params.nifi_ca_host and not params.nifi_ssl_enabled:
      params.nifi_properties = self.cleanup_toolkit_client_files(params, config_version_file)

    #get the last sensitive properties key for migration to new key if necessary
    if params.stack_support_encrypt_config:
      params.nifi_properties['nifi.sensitive.props.key'] = nifi_toolkit_util.get_last_sensitive_props_key(config_version_file,params.nifi_properties)

    #write out nifi.properties
    PropertiesFile(params.nifi_config_dir + '/nifi.properties', properties = params.nifi_properties, mode = 0600, owner = params.nifi_user, group = params.nifi_group)

    #write out boostrap.conf
    bootstrap_content=InlineTemplate(params.nifi_boostrap_content)
    File(format("{params.nifi_bootstrap_file}"), content=bootstrap_content, owner=params.nifi_user, group=params.nifi_group, mode=0600)

    #write out logback.xml
    logback_content=InlineTemplate(params.nifi_node_logback_content)
    File(format("{params.nifi_config_dir}/logback.xml"), content=logback_content, owner=params.nifi_user, group=params.nifi_group, mode=0400)

    #write out state-management.xml
    statemgmt_content=InlineTemplate(params.nifi_state_management_content)
    File(format("{params.nifi_config_dir}/state-management.xml"), content=statemgmt_content, owner=params.nifi_user, group=params.nifi_group, mode=0400)

    #write out authorizers file
    authorizers_content=InlineTemplate(params.nifi_authorizers_content)
    File(format("{params.nifi_config_dir}/authorizers.xml"), content=authorizers_content, owner=params.nifi_user, group=params.nifi_group, mode=0400)

    #write out login-identity-providers.xml
    login_identity_providers_content=InlineTemplate(params.nifi_login_identity_providers_content)
    File(format("{params.nifi_config_dir}/login-identity-providers.xml"), content=login_identity_providers_content, owner=params.nifi_user, group=params.nifi_group, mode=0600)

    #write out nifi-env in bin as 0755 (see BUG-61769)
    env_content=InlineTemplate(params.nifi_env_content)
    File(format("{params.bin_dir}/nifi-env.sh"), content=env_content, owner=params.nifi_user, group=params.nifi_group, mode=0755) 
    
    #write out bootstrap-notification-services.xml
    boostrap_notification_content=InlineTemplate(params.nifi_boostrap_notification_content)
    File(format("{params.nifi_config_dir}/bootstrap-notification-services.xml"), content=boostrap_notification_content, owner=params.nifi_user, group=params.nifi_group, mode=0400)

    #if security is enabled for kerberos create the nifi_jaas.conf file
    if params.security_enabled and params.stack_support_nifi_jaas:
      File(params.nifi_jaas_conf, content=InlineTemplate(params.nifi_jaas_conf_template), owner=params.nifi_user, group=params.nifi_group, mode=0400)

    if params.stack_support_encrypt_config:
      self.encrypt_sensitive_properties(config_version_file,params.nifi_ambari_config_version,
                                        params.nifi_config_dir,params.jdk64_home,params.nifi_user,
                                        params.nifi_group,params.nifi_security_encrypt_configuration_password,
                                        params.nifi_flow_config_dir, params.nifi_sensitive_props_key, is_starting)

    # Write out flow.xml.gz to internal dir only if AMS installed (must be writable by Nifi)
    # only during first install. It is used to automate setup of Ambari metrics reporting task in Nifi
    if params.metrics_collector_host and params.nifi_ambari_reporting_enabled and not sudo.path_isfile(params.nifi_flow_config_dir+'/flow.xml.gz'):
      Execute('echo "First time setup so generating flow.xml.gz" >> ' + params.nifi_node_log_file, user=params.nifi_user)
      flow_content=InlineTemplate(params.nifi_flow_content)
      File(format("{params.nifi_flow_config_dir}/flow.xml"), content=flow_content, owner=params.nifi_user, group=params.nifi_group, mode=0600)
      Execute(format("cd {params.nifi_flow_config_dir}; gzip flow.xml;"), user=params.nifi_user)


  def stop(self, env, upgrade_type=None):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)

    env_content=InlineTemplate(params.nifi_env_content)
    File(format("{params.bin_dir}/nifi-env.sh"), content=env_content, owner=params.nifi_user, group=params.nifi_group, mode=0755)

    Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh stop >> ' + params.nifi_node_log_file, user=params.nifi_user)
    if os.path.isfile(status_params.nifi_node_pid_file):
      sudo.unlink(status_params.nifi_node_pid_file)

  def start(self, env, upgrade_type=None):
    import params
    import status_params
    self.configure(env, is_starting = True)
    setup_ranger_nifi(upgrade_type=None)

    Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh start >> ' + params.nifi_node_log_file, user=params.nifi_user)
    #If nifi pid file not created yet, wait a bit
    if not os.path.isfile(status_params.nifi_pid_dir+'/nifi.pid'):
      Execute ('sleep 5')


  def status(self, env):
    import status_params
    check_process_status(status_params.nifi_node_pid_file)

  def setup_tls_toolkit_upgrade(self,env):
    import params
    env.set_params(params)

    upgrade_stack = stack_select._get_upgrade_stack()
    if upgrade_stack is None:
      raise Fail('Unable to determine the stack and stack version')

    if params.upgrade_direction == Direction.UPGRADE and params.nifi_ssl_enabled and params.nifi_ca_host:
      version_file = params.nifi_config_dir + '/config_version'
      client_json_file = params.nifi_config_dir+ '/nifi-certificate-authority-client.json'

      if not sudo.path_isfile(version_file):
        Logger.info(format('Create config version file if it does not exist'))
        version_num = params.config['configurationTags']['nifi-ambari-ssl-config']['tag']
        nifi_toolkit_util.save_config_version(version_file,'ssl',version_num,params.nifi_user,params.nifi_group)

      if sudo.path_isfile(client_json_file):
        Logger.info(format('Remove client json file'))
        sudo.unlink(client_json_file)

  def setup_keystore_truststore(self, is_starting, params, config_version_file):
    if is_starting:
      #check against last version to determine if key/trust has changed
      last_config_version = nifi_toolkit_util.get_config_version(config_version_file,'ssl')
      last_config = nifi_toolkit_util.get_config_by_version('/var/lib/ambari-agent/data','nifi-ambari-ssl-config',last_config_version)
      ca_client_dict = nifi_toolkit_util.get_nifi_ca_client_dict(last_config, params)
      using_client_json = len(ca_client_dict) == 0 and sudo.path_isfile(params.nifi_config_dir+ '/nifi-certificate-authority-client.json')

      if using_client_json:
        ca_client_dict = nifi_toolkit_util.load(params.nifi_config_dir + '/nifi-certificate-authority-client.json')

      changed_keystore_truststore = nifi_toolkit_util.changed_keystore_truststore(ca_client_dict,params.nifi_ca_client_config,using_client_json) if not len(ca_client_dict) == 0 else True

      if params.nifi_toolkit_tls_regenerate:
        nifi_toolkit_util.move_keystore_truststore(ca_client_dict)
        ca_client_dict = {}
      elif changed_keystore_truststore:
        nifi_toolkit_util.move_keystore_truststore(ca_client_dict)

      if changed_keystore_truststore or params.nifi_toolkit_tls_regenerate:
        nifi_toolkit_util.overlay(ca_client_dict, params.nifi_ca_client_config)
        updated_properties = self.run_toolkit_client(ca_client_dict,params.nifi_config_dir, params.jdk64_home, params.nifi_user,params.nifi_group, params.stack_support_toolkit_update)
        nifi_toolkit_util.update_nifi_properties(updated_properties, params.nifi_properties)
        nifi_toolkit_util.save_config_version(config_version_file,'ssl', params.nifi_ambari_ssl_config_version, params.nifi_user, params.nifi_group)
      elif using_client_json:
        nifi_toolkit_util.save_config_version(config_version_file,'ssl', params.nifi_ambari_ssl_config_version, params.nifi_user, params.nifi_group)

      old_nifi_properties = nifi_toolkit_util.convert_properties_to_dict(params.nifi_config_dir + '/nifi.properties')
      return nifi_toolkit_util.populate_ssl_properties(old_nifi_properties,params.nifi_properties,params)

    else:
      return params.nifi_properties

  def run_toolkit_client(self,ca_client_dict, nifi_config_dir, jdk64_home, nifi_user,nifi_group, no_client_file=False):
    Logger.info("Generating NiFi Keystore and Truststore")
    ca_client_script = nifi_toolkit_util.get_toolkit_script('tls-toolkit.sh')
    File(ca_client_script, mode=0755)
    if no_client_file:
      cert_command = 'echo \'' + json.dumps(ca_client_dict) + '\' | ambari-sudo.sh JAVA_HOME='+jdk64_home+' '+ ca_client_script + ' client -f /dev/stdout --configJsonIn /dev/stdin'
      code, out = shell.call(cert_command,quiet=True,logoutput=False)
      if code > 0:
        raise Fail("Call to tls-toolkit encountered error: {0}".format(out))
      else:
        json_out = out[out.index('{'):len(out)]
        updated_properties = json.loads(json_out)
        shell.call(['chown',nifi_user+':'+nifi_group,updated_properties['keyStore']],sudo=True)
        shell.call(['chown',nifi_user+':'+nifi_group,updated_properties['trustStore']],sudo=True)
    else:
      ca_client_json = os.path.realpath(os.path.join(nifi_config_dir, 'nifi-certificate-authority-client.json'))
      nifi_toolkit_util.dump(ca_client_json, ca_client_dict, nifi_user, nifi_group)
      Execute('JAVA_HOME='+jdk64_home+' '+ca_client_script+' client -F -f '+ca_client_json, user=nifi_user)
      updated_properties = nifi_toolkit_util.load(ca_client_json)

    return updated_properties

  def cleanup_toolkit_client_files(self, params,config_version_file):
    if nifi_toolkit_util.get_config_version(config_version_file,'ssl'):
      Logger.info("Search and remove any generated keystores and truststores")
      ca_client_dict = nifi_toolkit_util.get_nifi_ca_client_dict(params.config, params)
      nifi_toolkit_util.move_keystore_truststore(ca_client_dict)
      params.nifi_properties['nifi.security.keystore'] = ''
      params.nifi_properties['nifi.security.truststore'] = ''
      nifi_toolkit_util.remove_config_version(config_version_file,'ssl',params.nifi_user, params.nifi_group)

    return params.nifi_properties

  def encrypt_sensitive_properties(self,config_version_file,current_version,nifi_config_dir,jdk64_home,nifi_user,nifi_group,master_key_password,nifi_flow_config_dir,nifi_sensitive_props_key,is_starting):
    Logger.info("Encrypting NiFi sensitive configuration properties")
    encrypt_config_script = nifi_toolkit_util.get_toolkit_script('encrypt-config.sh')
    encrypt_config_script_prefix = ('JAVA_HOME='+jdk64_home,encrypt_config_script)
    File(encrypt_config_script, mode=0755)

    if is_starting:
      last_master_key_password = None
      last_config_version = nifi_toolkit_util.get_config_version(config_version_file,'encrypt')
      encrypt_config_script_params = ('-v','-b',nifi_config_dir+'/bootstrap.conf')
      encrypt_config_script_params = encrypt_config_script_params + ('-n',nifi_config_dir+'/nifi.properties')

      if sudo.path_isfile(nifi_flow_config_dir+'/flow.xml.gz') and len(sudo.read_file(nifi_flow_config_dir+'/flow.xml.gz')) > 0:
        encrypt_config_script_params = encrypt_config_script_params + ('-f',nifi_flow_config_dir+'/flow.xml.gz','-s',PasswordString(nifi_sensitive_props_key))

      if nifi_toolkit_util.contains_providers(nifi_config_dir+'/login-identity-providers.xml'):
        encrypt_config_script_params = encrypt_config_script_params + ('-l',nifi_config_dir+'/login-identity-providers.xml')

      if last_config_version:
        last_config = nifi_toolkit_util.get_config_by_version('/var/lib/ambari-agent/data','nifi-ambari-config',last_config_version)
        last_master_key_password = last_config['configurations']['nifi-ambari-config']['nifi.security.encrypt.configuration.password']

      if last_master_key_password and last_master_key_password != master_key_password:
        encrypt_config_script_params = encrypt_config_script_params + ('-m','-w',PasswordString(last_master_key_password))

      encrypt_config_script_params = encrypt_config_script_params + ('-p',PasswordString(master_key_password))
      encrypt_config_script_prefix = encrypt_config_script_prefix + encrypt_config_script_params
      Execute(encrypt_config_script_prefix, user=nifi_user,logoutput=False)
      nifi_toolkit_util.save_config_version(config_version_file,'encrypt', current_version, nifi_user, nifi_group)


if __name__ == "__main__":
  Master().execute()
