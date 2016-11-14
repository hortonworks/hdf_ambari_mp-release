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

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
  def get_component_name(self):
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
    import status_params

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
    Directory([status_params.nifi_pid_dir, params.nifi_node_log_dir, params.nifi_internal_dir, params.nifi_database_dir, params.nifi_flowfile_repo_dir, params.nifi_content_repo_dir_default, params.nifi_provenance_repo_dir_default, params.nifi_config_dir, params.nifi_flow_config_dir, params.nifi_state_dir, params.lib_dir],
            owner=params.nifi_user,
            group=params.nifi_group,
            create_parents=True,
            recursive_ownership=True
    )

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

    #write out nifi.properties
    PropertiesFile(params.nifi_config_dir + '/nifi.properties', properties = params.nifi_properties, mode = 0600, owner = params.nifi_user, group = params.nifi_group)

    #write out boostrap.conf
    bootstrap_content=InlineTemplate(params.nifi_boostrap_content)
    File(format("{params.nifi_config_dir}/bootstrap.conf"), content=bootstrap_content, owner=params.nifi_user, group=params.nifi_group, mode=0600)

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
    File(format("{params.nifi_config_dir}/login-identity-providers.xml"), content=login_identity_providers_content, owner=params.nifi_user, group=params.nifi_group, mode=0400)

    #write out nifi-env in bin as 0755 (see BUG-61769)
    env_content=InlineTemplate(params.nifi_env_content)
    File(format("{params.bin_dir}/nifi-env.sh"), content=env_content, owner=params.nifi_user, group=params.nifi_group, mode=0755) 
    
    #write out bootstrap-notification-services.xml
    boostrap_notification_content=InlineTemplate(params.nifi_boostrap_notification_content)
    File(format("{params.nifi_config_dir}/bootstrap-notification-services.xml"), content=boostrap_notification_content, owner=params.nifi_user, group=params.nifi_group, mode=0400)

    if params.stack_support_encrypt_config:
      self.encrypt_sensitive_properties(config_version_file,params.nifi_ambari_config_version,
                                        params.nifi_config_dir,params.jdk64_home,params.nifi_user,
                                        params.nifi_group,params.nifi_security_encrypt_configuration_password, is_starting)


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

    # Write out flow.xml.gz to internal dir only if AMS installed (must be writable by Nifi)
    # only during first install. It is used to automate setup of Ambari metrics reporting task in Nifi
    if params.metrics_collector_host and params.nifi_ambari_reporting_enabled and self.check_is_fresh_install(self):
      Execute('echo "First time setup so generating flow.xml.gz" >> ' + params.nifi_node_log_file, user=params.nifi_user)
      flow_content=InlineTemplate(params.nifi_flow_content)
      File(format("{params.nifi_flow_config_dir}/flow.xml"), content=flow_content, owner=params.nifi_user, group=params.nifi_group, mode=0600)
      Execute(format("cd {params.nifi_flow_config_dir}; mv flow.xml.gz flow_$(date +%d-%m-%Y).xml.gz ;"),user=params.nifi_user,ignore_failures=True)
      Execute(format("cd {params.nifi_flow_config_dir}; gzip flow.xml;"), user=params.nifi_user)


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
      changed_keystore_truststore = nifi_toolkit_util.changed_keystore_truststore(ca_client_dict,params.nifi_ca_client_config)

      if params.nifi_toolkit_tls_regenerate:
        nifi_toolkit_util.move_keystore_truststore(ca_client_dict)
        ca_client_dict = {}
      elif changed_keystore_truststore:
        nifi_toolkit_util.move_keystore_truststore(ca_client_dict)

      if changed_keystore_truststore or len(ca_client_dict) == 0:
        nifi_toolkit_util.overlay(ca_client_dict, params.nifi_ca_client_config)
        updated_properties = self.run_toolkit_client(ca_client_dict,params.nifi_config_dir, params.jdk64_home, params.nifi_user,params.nifi_group, params.stack_support_toolkit_update)
        nifi_toolkit_util.update_nifi_properties(updated_properties, params.nifi_properties)
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
      cert_command = 'echo \'' + json.dumps(ca_client_dict) + '\' | JAVA_HOME='+jdk64_home + ' ambari-sudo.sh ' + ca_client_script + ' client -f /dev/stdout --configJsonIn /dev/stdin'
      code, out = shell.call(cert_command,quiet=True,logoutput=False)
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

  def encrypt_sensitive_properties(self,config_version_file,current_version,nifi_config_dir,jdk64_home,nifi_user,nifi_group,master_key_password,is_starting):
    Logger.info("Encrypting NiFi sensitive configuration properties")
    encrypt_config_script = nifi_toolkit_util.get_toolkit_script('encrypt-config.sh')
    encrypt_config_script_prefix = ('JAVA_HOME='+jdk64_home,encrypt_config_script)
    File(encrypt_config_script, mode=0755)
    if is_starting:
      encrypt_config_script_params = ('-v','-b',nifi_config_dir+'/bootstrap.conf')
      encrypt_config_script_params = encrypt_config_script_params + ('-n',nifi_config_dir+'/nifi.properties')
      last_master_key_password = None
      last_config_version = nifi_toolkit_util.get_config_version(config_version_file,'encrypt')

      if last_config_version:
        last_config = nifi_toolkit_util.get_config_by_version('/var/lib/ambari-agent/data','nifi-ambari-config',last_config_version)
        last_master_key_password = last_config['configurations']['nifi-ambari-config']['nifi.security.encrypt.configuration.password']

      if last_master_key_password and last_master_key_password != master_key_password:
        encrypt_config_script_params = encrypt_config_script_params + ('-m','-w',PasswordString(last_master_key_password))

      encrypt_config_script_params = encrypt_config_script_params + ('-p',PasswordString(master_key_password))
      encrypt_config_script_prefix = encrypt_config_script_prefix + encrypt_config_script_params
      Execute(encrypt_config_script_prefix, user=nifi_user)
      nifi_toolkit_util.save_config_version(config_version_file,'encrypt', current_version, nifi_user, nifi_group)

  def check_is_fresh_install(self, env):
    """
    Checks if fresh nifi install by checking if zk dir exists
    :return:
    """
    import params, re
    from resource_management.core import shell
    from resource_management.core.exceptions import Fail
    from resource_management.core.logger import Logger

    ZK_CONNECT_ERROR = "ConnectionLoss"
    ZK_NODE_NOT_EXIST = "Node does not exist"

    zookeeper_queried = False
    is_fresh_nifi_install = True

    # For every zk server try to find nifi zk dir
    zookeeper_server_list = params.config['clusterHostInfo']['zookeeper_hosts']
    for zookeeper_server in zookeeper_server_list:
      # Determine where the zkCli.sh shell script is
      zk_command_location = os.path.join(params.stack_root, "current", "zookeeper-client", "bin", "zkCli.sh")
      if params.stack_version_buildnum is not None:
        zk_command_location = os.path.join(params.stack_root, params.stack_version_buildnum, "zookeeper", "bin", "zkCli.sh")

      # create the ZooKeeper query command e.g.
      # /usr/hdf/current/zookeeper-client/bin/zkCli.sh -server node:2181 ls /nifi
      command = "{0} -server {1}:{2} ls {3}".format(
        zk_command_location, zookeeper_server, params.zookeeper_port, params.nifi_znode)
              
      # echo 'ls /nifi' | /usr/hdf/current/zookeeper-client/bin/zkCli.sh -server node:2181
      #command = "echo 'ls {3}' | {0} -server {1}:{2}".format(
      #  zk_command_location, zookeeper_server, params.zookeeper_port, params.nifi_znode)

      Logger.info("Running command: " + command)

      code, out = shell.call(command, logoutput=True, quiet=False, timeout=20)
      if not out or re.search(ZK_CONNECT_ERROR, out):
        Logger.info("Unable to query Zookeeper: " + zookeeper_server + ". Skipping and trying next ZK server")
        continue
      elif re.search(ZK_NODE_NOT_EXIST, out):
        Logger.info("Nifi ZNode does not exist, so must be fresh install of Nifi: " + params.nifi_znode)
        zookeeper_queried = True
        is_fresh_nifi_install = True
        break
      else:
        Logger.info("Nifi ZNode already exists, so must not be a fresh install of Nifi: " + params.nifi_znode)
        zookeeper_queried = True
        is_fresh_nifi_install = False
        break

    # fail if the ZK data could not be queried
    if not zookeeper_queried:
      raise Fail("Unable to query for znode on on any of the following ZooKeeper hosts: {0}. Please ensure Zookeepers are started and retry".format(
        zookeeper_server_list))
    else:
      return is_fresh_nifi_install    
            

if __name__ == "__main__":
  Master().execute()
