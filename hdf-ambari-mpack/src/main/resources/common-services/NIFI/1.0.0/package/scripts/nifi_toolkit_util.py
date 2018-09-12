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
import json, nifi_constants, os, uuid, hashlib, hmac
from resource_management import *
from resource_management.core import sudo
from resource_management.core.resources.system import File, Directory
from resource_management.core.utils import PasswordString
from resource_management.core.source import StaticFile
from resource_management.core.logger import Logger
from resource_management.libraries.functions import format
from resource_management.libraries.functions.decorator import retry
from resource_management.core import shell
from resource_management.core.exceptions import Fail

script_dir = os.path.dirname(__file__)
files_dir = os.path.realpath(os.path.join(os.path.dirname(script_dir), 'files'))
param_delim = '||'

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
    mode=0640,
    content=PasswordString(json.dumps(config_dict, sort_keys=True, indent=4))
  )

def overlay(config_dict, overlay_dict):
  for k, v in overlay_dict.iteritems():
    if (k not in config_dict) or not(overlay_dict[k] == config_dict[k]):
      config_dict[k] = v

def get_toolkit_script(scriptName, scriptDir = files_dir, toolkitDirPrefix = 'nifi-toolkit-'):
  nifiToolkitDir = None
  for dir in os.listdir(scriptDir):
    if toolkitDirPrefix in dir and dir.startswith('nifi-toolkit'):
      nifiToolkitDir = os.path.join(scriptDir, dir)

  if nifiToolkitDir is None:
    raise Exception("Couldn't find nifi toolkit directory in " + scriptDir)
  result = nifiToolkitDir + '/bin/' + scriptName
  if not sudo.path_isfile(result):
    raise Exception("Couldn't find file " + result)
  return result

def copy_toolkit_scripts(toolkit_files_dir, toolkit_tmp_dir, user, group, upgrade_type):
  run_ca_tmp_script = os.path.join(toolkit_tmp_dir,'run_ca.sh')
  new_run_ca_tmp_script = StaticFile("run_ca.sh")

  if not sudo.path_isfile(run_ca_tmp_script) or sudo.read_file(run_ca_tmp_script) != new_run_ca_tmp_script:
    File(format(run_ca_tmp_script), content=new_run_ca_tmp_script, mode=0755,owner=user, group=group)

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

def update_nifi_ca_properties(client_dict, nifi_properties):
  nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_TYPE] = client_dict['keyStoreType']
  nifi_properties[nifi_constants.NIFI_SECURITY_KEYSTORE_PASSWD] = client_dict['keyStorePassword']
  nifi_properties[nifi_constants.NIFI_SECURITY_KEY_PASSWD] = client_dict['keyPassword']
  nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_TYPE] = client_dict['trustStoreType']
  nifi_properties[nifi_constants.NIFI_SECURITY_TRUSTSTORE_PASSWD] = client_dict['trustStorePassword']

def update_nifi_ssl_properties(nifi_properties, nifi_truststore, nifi_node_ssl_host, nifi_config_dir, nifi_truststoreType, nifi_truststorePasswd,
                               nifi_keystore, nifi_keystoreType, nifi_keystorePasswd,nifi_keyPasswd):
  nifi_properties['nifi.security.truststore'] = nifi_truststore.replace('{nifi_node_ssl_host}', nifi_node_ssl_host).replace('{{nifi_config_dir}}', nifi_config_dir)
  nifi_properties['nifi.security.truststoreType'] = nifi_truststoreType
  nifi_properties['nifi.security.truststorePasswd'] = nifi_truststorePasswd
  nifi_properties['nifi.security.keystore'] = nifi_keystore.replace('{nifi_node_ssl_host}', nifi_node_ssl_host).replace('{{nifi_config_dir}}', nifi_config_dir)
  nifi_properties['nifi.security.keystoreType'] = nifi_keystoreType
  nifi_properties['nifi.security.keystorePasswd'] = nifi_keystorePasswd
  nifi_properties['nifi.security.keyPasswd'] = nifi_keyPasswd
  return nifi_properties

def update_nifi_ambari_hash_properties(nifi_truststorePasswd, nifi_keystorePasswd, nifi_keyPasswd, master_key):
  nifi_properties = {}
  nifi_properties['#nifi.security.ambari.hash.kspwd'] = hash(nifi_keystorePasswd, master_key)
  nifi_properties['#nifi.security.ambari.hash.kpwd'] = hash(nifi_keyPasswd, master_key)
  nifi_properties['#nifi.security.ambari.hash.tspwd'] = hash(nifi_truststorePasswd, master_key)
  return nifi_properties

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

def hash(value,master_key):
  m = hashlib.sha512()
  m.update(master_key)
  derived_key = m.hexdigest()[0:32]
  h = hmac.new(derived_key, value, hashlib.sha256)
  return h.hexdigest()

def match(a,b):
  if len(a) != len(b):
    return False
  result = 0
  for x, y in zip(a, b):
    result |= int(x,base=16) ^ int(y,base=16)
  return result == 0

def generate_keystore_truststore(orig_client_dict, new_client_dict, master_key):
  if not (store_exists(new_client_dict, 'nifi.security.keystore') and store_exists(new_client_dict, 'nifi.security.truststore')):
    return True
  elif orig_client_dict['nifi.security.keystoreType'] != new_client_dict['nifi.security.keystoreType']:
    return True
  elif ('#nifi.security.ambari.hash.kspwd' not in orig_client_dict) or not match(orig_client_dict['#nifi.security.ambari.hash.kspwd'], hash(new_client_dict['nifi.security.keystorePasswd'], master_key)):
    return True
  elif ('#nifi.security.ambari.hash.kpwd' not in orig_client_dict) or not match(orig_client_dict['#nifi.security.ambari.hash.kpwd'], hash(new_client_dict['nifi.security.keyPasswd'], master_key)):
    return True
  elif orig_client_dict['nifi.security.truststoreType'] != new_client_dict['nifi.security.truststoreType']:
    return True
  elif ('#nifi.security.ambari.hash.tspwd' not in orig_client_dict) or not match(orig_client_dict['#nifi.security.ambari.hash.tspwd'], hash(new_client_dict['nifi.security.truststorePasswd'],master_key)):
    return True
  elif orig_client_dict['nifi.security.keystore'] != new_client_dict['nifi.security.keystore']:
    return True
  elif orig_client_dict['nifi.security.truststore'] != new_client_dict['nifi.security.truststore']:
    return True
  else:
    return False

def move_keystore_truststore(client_dict):
  move_store(client_dict, 'nifi.security.keystore')
  move_store(client_dict, 'nifi.security.truststore')

def move_store(client_dict, key):
  if store_exists(client_dict, key):
    num = 0
    name = client_dict[key]
    while sudo.path_isfile(name + '.bak.' + str(num)):
      num += 1
    sudo.copy(name, name + '.bak.' + str(num))
    sudo.unlink(name)

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


def create_keystore_truststore(is_starting, params):
  if is_starting:
    updated_properties = run_toolkit_client(get_nifi_ca_client_dict(params.config, params), params.nifi_config_dir,
                                            params.jdk64_home, params.nifi_toolkit_java_options,
                                            params.nifi_user, params.nifi_group,
                                            params.toolkit_tmp_dir, params.stack_version_buildnum, params.stack_support_toolkit_update)

    update_nifi_ca_properties(updated_properties, params.nifi_properties)

  return params.nifi_properties

@retry(times=20, sleep_time=5, max_sleep_time=20, backoff_factor=2, err_class=Fail)
def run_toolkit_client(ca_client_dict, nifi_config_dir, jdk64_home, java_options, nifi_user,nifi_group,toolkit_tmp_dir, stack_version_buildnum, no_client_file=False):
  Logger.info("Generating NiFi Keystore and Truststore")
  ca_client_script = get_toolkit_script('tls-toolkit.sh',toolkit_tmp_dir, stack_version_buildnum)
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

def clean_toolkit_client_files(old_nifi_properties, new_nifi_properties):
  move_keystore_truststore(old_nifi_properties)
  new_nifi_properties['nifi.security.keystore'] = ''
  new_nifi_properties['nifi.security.truststore'] = ''
  return new_nifi_properties


def encrypt_sensitive_properties(nifi_config_dir, jdk64_home, java_options, nifi_user, last_master_key, master_key_password, nifi_flow_config_dir, nifi_sensitive_props_key, is_starting,toolkit_tmp_dir, support_encrypt_authorizers, stack_version_buildnum):
  Logger.info("Encrypting NiFi sensitive configuration properties")
  encrypt_config_script = get_toolkit_script('encrypt-config.sh',toolkit_tmp_dir, stack_version_buildnum)
  encrypt_config_command = (encrypt_config_script,)
  environment = {'JAVA_HOME': jdk64_home, 'JAVA_OPTS': java_options}
  File(encrypt_config_script, mode=0755)

  if is_starting:

    encrypt_config_command += ('-v', '-b', nifi_config_dir + '/bootstrap.conf')
    encrypt_config_command += ('-n', nifi_config_dir + '/nifi.properties')

    if (sudo.path_isfile(nifi_flow_config_dir + '/flow.xml.gz')
            and len(sudo.read_file(nifi_flow_config_dir + '/flow.xml.gz')) > 0):
      encrypt_config_command += ('-f', nifi_flow_config_dir + '/flow.xml.gz', '-s', PasswordString(nifi_sensitive_props_key))

    if contains_providers(nifi_config_dir+'/login-identity-providers.xml', "provider"):
      encrypt_config_command += ('-l', nifi_config_dir + '/login-identity-providers.xml')

    if support_encrypt_authorizers and contains_providers(nifi_config_dir+'/authorizers.xml', "authorizer"):
      encrypt_config_command += ('-a', nifi_config_dir + '/authorizers.xml')

    if last_master_key:
      encrypt_config_command += ('-m', '-e', PasswordString(last_master_key))

    encrypt_config_command += ('-p', PasswordString(master_key_password))
    Execute(encrypt_config_command, user=nifi_user, logoutput=False, environment=environment)

def get_client_opts():
  import params
  encrypt_config_script = get_toolkit_script('encrypt-config.sh', params.toolkit_tmp_dir, params.stack_version_buildnum)
  environment = {'JAVA_HOME': params.jdk64_home, 'JAVA_OPTS': params.nifi_toolkit_java_options}
  command_args = (encrypt_config_script, '-c', '-b', params.nifi_config_dir + '/bootstrap.conf', '-n', params.nifi_config_dir + '/nifi.properties')
  code, out = shell.call(command_args, env=environment, logoutput=False, quiet=True, user=params.nifi_user)
  if code == 0:
    result = {}
    for line in [l for l in out.splitlines() if l]:
      try:
        name, value = line.split("=")
        result[name] = value
      except ValueError:
        pass
    return result
  else:
    raise Fail("Unable to get parameters for client.")