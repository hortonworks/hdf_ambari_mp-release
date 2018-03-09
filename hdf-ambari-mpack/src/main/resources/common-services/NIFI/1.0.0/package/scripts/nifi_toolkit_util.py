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
import json, nifi_constants, os
from resource_management import *
from resource_management.core import sudo
from resource_management.core.resources.system import File, Directory
from resource_management.core.utils import PasswordString
from resource_management.core.source import StaticFile
from resource_management.core.logger import Logger
from resource_management.libraries.functions import format
from resource_management.libraries.functions.decorator import retry
from subprocess import call

script_dir = os.path.dirname(__file__)
files_dir = os.path.realpath(os.path.join(os.path.dirname(script_dir), 'files'))

def load(config_json):
  if sudo.path_isfile(config_json):
    contents = sudo.read_file(config_json)
    if len(contents) > 0:
      return json.loads(contents)
  return {}

def dump(config_json, config_dict, nifi_user, nifi_group):

  File(config_json,
    owner=nifi_user,
    group=nifi_group,
    mode=0644,
    content=PasswordString(json.dumps(config_dict, sort_keys=True, indent=4))
  )

def overlay(config_dict, overlay_dict):
  for k, v in overlay_dict.iteritems():
    if (k not in config_dict) or not(overlay_dict[k] == config_dict[k]):
      config_dict[k] = v

def get_toolkit_script(scriptName, scriptDir = files_dir):
  nifiToolkitDir = None
  for dir in os.listdir(scriptDir):
    if dir.startswith('nifi-toolkit-'):
      nifiToolkitDir = os.path.join(scriptDir, dir)

  if nifiToolkitDir is None:
    raise Exception("Couldn't find nifi toolkit directory in " + scriptDir)
  result = nifiToolkitDir + '/bin/' + scriptName
  if not sudo.path_isfile(result):
    raise Exception("Couldn't find file " + result)
  return result

def copy_toolkit_scripts(toolkit_files_dir, toolkit_tmp_dir, user, group, upgrade_type):
  run_ca_tmp_script = os.path.join(toolkit_tmp_dir,'run_ca.sh')

  if not sudo.path_isfile(run_ca_tmp_script) or not (upgrade_type is None):
    File(format(run_ca_tmp_script),
         content=StaticFile("run_ca.sh"),
         mode=0755,owner=user, group=group)

  nifiToolkitDirFilesPath = None
  nifiToolkitDirTmpPath = None

  for dir in os.listdir(toolkit_files_dir):
    if dir.startswith('nifi-toolkit-'):
      nifiToolkitDirFilesPath = os.path.join(toolkit_files_dir, dir)
      nifiToolkitDirTmpPath = os.path.join(toolkit_tmp_dir, dir)

  if not sudo.path_isdir(nifiToolkitDirTmpPath) or not (upgrade_type is None):
    os.system("\cp -r " + nifiToolkitDirFilesPath+ " " + toolkit_tmp_dir)
    Directory(nifiToolkitDirTmpPath, owner=user, group=group, create_parents=False, recursive_ownership=True, cd_access="a", mode=0755)
    os.system("\/var/lib/ambari-agent/ambari-sudo.sh chmod -R 755 " + nifiToolkitDirTmpPath)

def update_nifi_properties(client_dict, nifi_properties):
  nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_TYPE] = client_dict['keyStoreType']
  nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_PASSWD] = client_dict['keyStorePassword']
  nifi_properties[nifi_constants.NIFI_SECURITY_KEY_PASSWD] = client_dict['keyPassword']
  nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_TYPE] = client_dict['trustStoreType']
  nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_PASSWD] = client_dict['trustStorePassword']

def store_exists(client_dict, key):
  if key not in client_dict:
    return False
  return sudo.path_isfile(client_dict[key])

def different(one, two, key, usingJsonConfig=False):
  if key not in one:
    return False
  if len(one[key]) == 0 and usingJsonConfig:
    return False
  if key not in two:
    return False
  if len(two[key]) == 0 and usingJsonConfig:
    return False
  return one[key] != two[key]

def changed_keystore_truststore(orig_client_dict, new_client_dict, usingJsonConfig=False):
  if not (store_exists(new_client_dict, 'keyStore') or store_exists(new_client_dict, 'trustStore')):
    return False
  elif different(orig_client_dict, new_client_dict, 'keyStoreType',usingJsonConfig):
    return True
  elif different(orig_client_dict, new_client_dict, 'keyStorePassword',usingJsonConfig):
    return True
  elif different(orig_client_dict, new_client_dict, 'keyPassword',usingJsonConfig):
    return True
  elif different(orig_client_dict, new_client_dict, 'trustStoreType',usingJsonConfig):
    return True
  elif different(orig_client_dict, new_client_dict, 'trustStorePassword',usingJsonConfig):
    return True

def move_keystore_truststore(client_dict):
  move_store(client_dict, 'keyStore')
  move_store(client_dict, 'trustStore')

def move_store(client_dict, key):
  if store_exists(client_dict, key):
    num = 0
    name = client_dict[key]
    while sudo.path_isfile(name + '.bak.' + str(num)):
      num += 1
    sudo.copy(name, name + '.bak.' + str(num))
    sudo.unlink(name)

def save_config_version(config, version_file,version_type,nifi_user,nifi_group):
  version = {}
  if sudo.path_isfile(version_file):
    contents = sudo.read_file(version_file)
    version = json.loads(contents)
    version[version_type] = config
    sudo.unlink(version_file)
  else:
    version[version_type] = config

  File(version_file,
       owner=nifi_user,
       group=nifi_group,
       mode=0600,
       content=json.dumps(version))

def get_config_version(version_file,version_type):
  if sudo.path_isfile(version_file):
    contents = sudo.read_file(version_file)
    version = json.loads(contents)
    if version_type in version:
      return version[version_type]
    else:
      return None
  else:
    return None

def remove_config_version(version_file,version_type, nifi_user, nifi_group):
  if sudo.path_isfile(version_file):
    contents = sudo.read_file(version_file)
    version = json.loads(contents)
    version.pop(version_type, None)
    sudo.unlink(version_file)

    File(version_file,
         owner=nifi_user,
         group=nifi_group,
         mode=0600,
         content=json.dumps(version))

def convert_properties_to_dict(prop_file):
  dict = {}
  if sudo.path_isfile(prop_file):
    lines = sudo.read_file(prop_file).split('\n')
    for line in lines:
      props = line.rstrip().split('=')
      if len(props) == 2:
        dict[props[0]] = props[1]
      elif len(props) == 1:
        dict[props[0]] = ''
  return dict

def populate_ssl_properties(old_prop,new_prop,params):

  if old_prop and len(old_prop) > 0:

    newKeyPasswd = new_prop['nifi.security.keyPasswd'].replace('{{nifi_keyPasswd}}',params.nifi_keyPasswd)
    newKeystorePasswd = new_prop['nifi.security.keystorePasswd'].replace('{{nifi_keystorePasswd}}',params.nifi_keystorePasswd)
    newTruststorePasswd = new_prop['nifi.security.truststorePasswd'].replace('{{nifi_truststorePasswd}}',params.nifi_truststorePasswd)

    if len(newKeyPasswd) == 0 and len(old_prop['nifi.security.keyPasswd']) > 0:
      new_prop['nifi.security.keyPasswd'] = old_prop['nifi.security.keyPasswd']
      if 'nifi.security.keyPasswd.protected' in old_prop:
        new_prop['nifi.security.keyPasswd.protected'] = old_prop['nifi.security.keyPasswd.protected']

    if len(newKeystorePasswd) == 0 and len(old_prop['nifi.security.keystorePasswd']) > 0:
      new_prop['nifi.security.keystorePasswd'] = old_prop['nifi.security.keystorePasswd']
      if 'nifi.security.keystorePasswd.protected' in old_prop:
        new_prop['nifi.security.keystorePasswd.protected'] = old_prop['nifi.security.keystorePasswd.protected']

    if len(newTruststorePasswd) == 0 and len(old_prop['nifi.security.truststorePasswd']) > 0 :
      new_prop['nifi.security.truststorePasswd'] = old_prop['nifi.security.truststorePasswd']
      if 'nifi.security.truststorePasswd.protected' in old_prop:
        new_prop['nifi.security.truststorePasswd.protected'] = old_prop['nifi.security.truststorePasswd.protected']

  return new_prop

def get_nifi_ca_client_dict(config,params):

  if not config or len(config) == 0:
    return {}
  else:
    nifi_keystore = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keystore']
    nifi_keystoreType = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keystoreType']
    nifi_keystorePasswd = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keystorePasswd']
    nifi_keyPasswd = config['configurations']['nifi-ambari-ssl-config']['nifi.security.keyPasswd']
    nifi_truststore = config['configurations']['nifi-ambari-ssl-config']['nifi.security.truststore']
    nifi_truststoreType = config['configurations']['nifi-ambari-ssl-config']['nifi.security.truststoreType']
    nifi_truststorePasswd = config['configurations']['nifi-ambari-ssl-config']['nifi.security.truststorePasswd']
    nifi_truststore = nifi_truststore.replace('{nifi_node_ssl_host}',params.nifi_node_host)
    nifi_truststore = nifi_truststore.replace('{{nifi_config_dir}}',params.nifi_config_dir)
    nifi_keystore = nifi_keystore.replace('{nifi_node_ssl_host}',params.nifi_node_host)
    nifi_keystore = nifi_keystore.replace('{{nifi_config_dir}}',params.nifi_config_dir)

    #default keystore/truststore type if empty
    nifi_keystoreType = 'jks' if len(nifi_keystoreType) == 0 else nifi_keystoreType
    nifi_truststoreType = 'jks' if len(nifi_truststoreType) == 0 else nifi_truststoreType

    nifi_toolkit_dn_prefix = config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.dn.prefix']
    nifi_toolkit_dn_suffix = config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.dn.suffix']

    nifi_ca_client_config = {
      "days" : int(config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.tls.helper.days']),
      "keyStore" : nifi_keystore,
      "keyStoreType" : nifi_keystoreType,
      "keyStorePassword" : nifi_keystorePasswd,
      "keyPassword" : nifi_keyPasswd,
      "token" : config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.tls.token'],
      "dn" : nifi_toolkit_dn_prefix + params.nifi_node_host + nifi_toolkit_dn_suffix,
      "port" : int(config['configurations']['nifi-ambari-ssl-config']['nifi.toolkit.tls.port']),
      "caHostname" : params.nifi_ca_host,
      "trustStore" : nifi_truststore,
      "trustStoreType" : nifi_truststoreType,
      "trustStorePassword": nifi_truststorePasswd
    }

    return nifi_ca_client_config

def get_last_sensitive_props_key(config_version_file,nifi_properties):
  last_encrypt_config = get_config_version(config_version_file,'encrypt')
  if last_encrypt_config:
    return last_encrypt_config['configurations']['nifi-ambari-config']['nifi.sensitive.props.key']
  else:
    return nifi_properties['nifi.sensitive.props.key']

def contains_providers(provider_file, tag):
  from xml.dom.minidom import parseString
  import xml.dom.minidom

  if sudo.path_isfile(provider_file):
    content = sudo.read_file(provider_file)
    dom = xml.dom.minidom.parseString(content)
    collection = dom.documentElement
    if collection.getElementsByTagName(tag):
      return True
    else:
      return False

  else:
    return False

def existing_cluster(params):

  import re

  ZK_CONNECT_ERROR = "ConnectionLoss"
  ZK_NODE_NOT_EXIST = "Node does not exist"

  if params.security_enabled:
    kinit_cmd = "{0} -kt {1} {2}; ".format(params.kinit_path_local, params.nifi_properties['nifi.kerberos.service.keytab.location'], params.nifi_properties['nifi.kerberos.service.principal'])
  else:
    kinit_cmd = ""

  # For every zk server try to find nifi zk dir
  zookeeper_server_list = params.config['clusterHostInfo'][params.zk_hosts_property]

  for zookeeper_server in zookeeper_server_list:

    # Determine where the zkCli.sh shell script is
    # When we are on HDP the stack_root will be /usr/hdf, but ZK will be in /usr/hdp, so use zk_root and not stack_root
    zk_command_location = os.path.join(params.zk_root, "current", "zookeeper-client", "bin", "zkCli.sh")

    if params.stack_version_buildnum is not None:
      zk_command_location = os.path.join(params.zk_root, params.zk_stack_version_buildnum, "zookeeper", "bin", "zkCli.sh")

    # create the ZooKeeper query command e.g.
    command = "{0} -server {1}:{2} ls {3}".format(zk_command_location, zookeeper_server, params.zookeeper_port, params.nifi_znode)

    Logger.info("Running command: " + command)

    code, out = shell.call( kinit_cmd + command, logoutput=True, quiet=False, timeout=20)

    if not out or re.search(ZK_CONNECT_ERROR, out):
      Logger.info("Unable to query Zookeeper: " + zookeeper_server + ". Skipping and trying next ZK server")
      continue
    elif re.search(ZK_NODE_NOT_EXIST, out):
      Logger.info("Nifi ZNode does not exist, so no pre-existing cluster.: " + params.nifi_znode)
      return False
    else:
      Logger.info("Nifi ZNode exists, so a cluster is defined: " + params.nifi_znode)
      return True

  return False


def setup_keystore_truststore(is_starting, params, config_version_file):
  if is_starting:
    #check against last version to determine if key/trust has changed
    last_config = get_config_version(config_version_file,'ssl')
    ca_client_dict = get_nifi_ca_client_dict(last_config, params)
    using_client_json = len(ca_client_dict) == 0 and sudo.path_isfile(params.nifi_config_dir+ '/nifi-certificate-authority-client.json')

    if using_client_json:
      ca_client_dict = load(params.nifi_config_dir + '/nifi-certificate-authority-client.json')

    changed_ks_ts = changed_keystore_truststore(ca_client_dict,params.nifi_ca_client_config,using_client_json) if not len(ca_client_dict) == 0 else True

    if params.nifi_toolkit_tls_regenerate:
      move_keystore_truststore(ca_client_dict)
      ca_client_dict = {}
    elif changed_ks_ts:
      move_keystore_truststore(ca_client_dict)

    if changed_ks_ts or params.nifi_toolkit_tls_regenerate:
      overlay(ca_client_dict, params.nifi_ca_client_config)
      updated_properties = run_toolkit_client(ca_client_dict, params.nifi_config_dir,
                                              params.jdk64_home, params.nifi_toolkit_java_options,
                                              params.nifi_user, params.nifi_group,
                                              params.toolkit_tmp_dir, params.stack_support_toolkit_update)
      update_nifi_properties(updated_properties, params.nifi_properties)
      save_config_version(params.config, config_version_file,'ssl', params.nifi_user, params.nifi_group)
    elif using_client_json:
      save_config_version(params.config, config_version_file,'ssl', params.nifi_user, params.nifi_group)

    old_nifi_properties = convert_properties_to_dict(params.nifi_config_dir + '/nifi.properties')
    return populate_ssl_properties(old_nifi_properties,params.nifi_properties,params)

  else:
    return params.nifi_properties

@retry(times=20, sleep_time=5, max_sleep_time=20, backoff_factor=2, err_class=Fail)
def run_toolkit_client(ca_client_dict, nifi_config_dir, jdk64_home, java_options, nifi_user,nifi_group,toolkit_tmp_dir, no_client_file=False):
  Logger.info("Generating NiFi Keystore and Truststore")
  ca_client_script = get_toolkit_script('tls-toolkit.sh',toolkit_tmp_dir)
  File(ca_client_script, mode=0755)
  if no_client_file:
    ca_client_json_dump = json.dumps(ca_client_dict)
    cert_command = (
        'echo \'%(ca_client_json_dump)s\''
        ' | ambari-sudo.sh'
        ' JAVA_HOME="%(jdk64_home)s"'
        ' JAVA_OPTS="%(java_options)s"'
        ' %(ca_client_script)s'
        ' client -f /dev/stdout --configJsonIn /dev/stdin'
    ) % locals()
    code, out = shell.call(cert_command, quiet=True, logoutput=False)
    if code > 0:
      raise Fail("Call to tls-toolkit encountered error: {0}".format(out))
    else:
      json_out = out[out.index('{'):len(out)]
      updated_properties = json.loads(json_out)
      shell.call(['chown',nifi_user+':'+nifi_group,updated_properties['keyStore']],sudo=True)
      shell.call(['chown',nifi_user+':'+nifi_group,updated_properties['trustStore']],sudo=True)
  else:
    ca_client_json = os.path.realpath(os.path.join(nifi_config_dir, 'nifi-certificate-authority-client.json'))
    dump(ca_client_json, ca_client_dict, nifi_user, nifi_group)
    environment = {'JAVA_HOME': jdk64_home, 'JAVA_OPTS': java_options}
    Execute((ca_client_script, 'client', '-F', '-f', ca_client_json), user=nifi_user, environment=environment)
    updated_properties = load(ca_client_json)

  return updated_properties

def cleanup_toolkit_client_files(params,config_version_file):
  if get_config_version(config_version_file,'ssl'):
    Logger.info("Search and remove any generated keystores and truststores")
    ca_client_dict = get_nifi_ca_client_dict(params.config, params)
    move_keystore_truststore(ca_client_dict)
    params.nifi_properties['nifi.security.keystore'] = ''
    params.nifi_properties['nifi.security.truststore'] = ''
    remove_config_version(config_version_file,'ssl',params.nifi_user, params.nifi_group)

  return params.nifi_properties

def encrypt_sensitive_properties(config, config_version_file, nifi_config_dir,jdk64_home,java_options,nifi_user,nifi_group,master_key_password,nifi_flow_config_dir,nifi_sensitive_props_key,
                                 is_starting,toolkit_tmp_dir,support_encrypt_authorizers):
  Logger.info("Encrypting NiFi sensitive configuration properties")
  encrypt_config_script = get_toolkit_script('encrypt-config.sh',toolkit_tmp_dir)

  encrypt_config_command = (encrypt_config_script,)
  environment = {'JAVA_HOME': jdk64_home, 'JAVA_OPTS': java_options}
  File(encrypt_config_script, mode=0755)

  if is_starting:
    last_config = get_config_version(config_version_file,'encrypt')
    encrypt_config_command += ('-v', '-b', nifi_config_dir + '/bootstrap.conf')
    encrypt_config_command += ('-n', nifi_config_dir + '/nifi.properties')

    if (sudo.path_isfile(nifi_flow_config_dir + '/flow.xml.gz')
            and len(sudo.read_file(nifi_flow_config_dir + '/flow.xml.gz')) > 0):
      encrypt_config_command += ('-f', nifi_flow_config_dir + '/flow.xml.gz', '-s', PasswordString(nifi_sensitive_props_key))

    if contains_providers(nifi_config_dir+'/login-identity-providers.xml', "provider"):
      encrypt_config_command += ('-l', nifi_config_dir + '/login-identity-providers.xml')

    if support_encrypt_authorizers and contains_providers(nifi_config_dir+'/authorizers.xml', "authorizer"):
      encrypt_config_command += ('-a', nifi_config_dir + '/authorizers.xml')

    last_master_key_password = None

    if last_config:
      last_master_key_password = last_config['configurations']['nifi-ambari-config']['nifi.security.encrypt.configuration.password']

    #The last master key password is required in order to migrate to the new password (if it was changed)
    if last_master_key_password and last_master_key_password != master_key_password:
      encrypt_config_command += ('-m', '-w', PasswordString(last_master_key_password))

    encrypt_config_command += ('-p', PasswordString(master_key_password))
    Execute(encrypt_config_command, user=nifi_user, logoutput=False, environment=environment)
    save_config_version(config, config_version_file, 'encrypt', nifi_user, nifi_group)
