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
import json, nifi_registry_constants, os, uuid, hashlib, hmac
from resource_management import *
from resource_management.core import sudo
from resource_management.core.resources.system import File, Directory
from resource_management.core.utils import PasswordString
from resource_management.core.logger import Logger
from resource_management.libraries.functions.decorator import retry


script_dir = os.path.dirname(__file__)
files_dir = os.path.realpath(os.path.join(os.path.dirname(script_dir), 'files'))
param_delim = '||'

def load(config_json):
    if sudo.path_isfile(config_json):
        contents = sudo.read_file(config_json)
        if len(contents) > 0:
            return json.loads(contents)
    return {}

def dump(config_json, config_dict, nifi_registry_user, nifi_registry_group):

    File(config_json,
         owner=nifi_registry_user,
         group=nifi_registry_group,
         mode=0600,
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
    nifiToolkitDirFilesPath = None
    nifiToolkitDirTmpPath = None

    Logger.info("Toolkit files dir is " + toolkit_files_dir)
    Logger.info("Toolkit tmp dir is " + toolkit_tmp_dir)

    for dir in os.listdir(toolkit_files_dir):
        if dir.startswith('nifi-toolkit-'):
            nifiToolkitDirFilesPath = os.path.join(toolkit_files_dir, dir)
            nifiToolkitDirTmpPath = os.path.join(toolkit_tmp_dir, dir)

    if not sudo.path_isdir(nifiToolkitDirTmpPath) or not (upgrade_type is None):
        os.system("\cp -r " + nifiToolkitDirFilesPath+ " " + toolkit_tmp_dir)
        Directory(nifiToolkitDirTmpPath, owner=user, group=group, create_parents=False, recursive_ownership=True, cd_access="a", mode=0755)
        os.system("\/var/lib/ambari-agent/ambari-sudo.sh chmod -R 755 " + nifiToolkitDirTmpPath)

def update_nifi_ca_registry_properties(client_dict, nifi_registry_properties):
    nifi_registry_properties[nifi_registry_constants.NIFI_REGISTRY_SECURITY_KEYSTORE_TYPE] = client_dict['keyStoreType']
    nifi_registry_properties[nifi_registry_constants.NIFI_REGISTRY_SECURITY_KEYSTORE_PASSWD] = client_dict['keyStorePassword']
    nifi_registry_properties[nifi_registry_constants.NIFI_REGISTRY_SECURITY_KEY_PASSWD] = client_dict['keyPassword']
    nifi_registry_properties[nifi_registry_constants.NIFI_REGISTRY_SECURITY_TRUSTSTORE_TYPE] = client_dict['trustStoreType']
    nifi_registry_properties[nifi_registry_constants.NIFI_REGISTRY_SECURITY_TRUSTSTORE_PASSWD] = client_dict['trustStorePassword']

def update_nifi_registry_ssl_properties(nifi_registry_properties, nifi_registry_truststore, nifi_registry_ssl_host, nifi_registry_config_dir,
                                        nifi_registry_truststoreType, nifi_registry_truststorePasswd, nifi_registry_keystore,
                                        nifi_registry_keystoreType, nifi_registry_keystorePasswd, nifi_registry_keyPasswd):

    nifi_registry_properties['nifi.registry.security.truststore'] = nifi_registry_truststore.replace('{nifi_registry_ssl_host}', nifi_registry_ssl_host).replace('{{nifi_registry_config_dir}}', nifi_registry_config_dir)
    nifi_registry_properties['nifi.registry.security.truststoreType'] = nifi_registry_truststoreType
    nifi_registry_properties['nifi.registry.security.truststorePasswd'] = nifi_registry_truststorePasswd
    nifi_registry_properties['nifi.registry.security.keystore'] = nifi_registry_keystore.replace('{nifi_registry_ssl_host}', nifi_registry_ssl_host).replace('{{nifi_config_dir}}', nifi_registry_config_dir)
    nifi_registry_properties['nifi.registry.security.keystoreType'] = nifi_registry_keystoreType
    nifi_registry_properties['nifi.registry.security.keystorePasswd'] = nifi_registry_keystorePasswd
    nifi_registry_properties['nifi.registry.security.keyPasswd'] = nifi_registry_keyPasswd
    return nifi_registry_properties

def update_nifi_registry_ambari_hash_properties(nifi_registry_truststorePasswd, nifi_registry_keystorePasswd, nifi_registry_keyPasswd, master_key):
    nifi_registry_properties = {}
    nifi_registry_properties['#nifi.registry.security.ambari.hash.kspwd'] = hash(nifi_registry_keystorePasswd, master_key)
    nifi_registry_properties['#nifi.registry.security.ambari.hash.kpwd']  = hash(nifi_registry_keyPasswd, master_key)
    nifi_registry_properties['#nifi.registry.security.ambari.hash.tspwd'] = hash(nifi_registry_truststorePasswd, master_key)
    return nifi_registry_properties

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

def hash(value, master_key):
    m = hashlib.sha512()
    m.update(master_key)
    derived_key = m.hexdigest()[0:32]
    h = hmac.new(derived_key, value, hashlib.sha256)
    return h.hexdigest()

def match(a, b):
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= int(x,base=16) ^ int(y,base=16)
    return result == 0

def generate_keystore_truststore(orig_client_dict, new_client_dict, master_key):
    if not (store_exists(new_client_dict, 'nifi.registry.security.keystore') and store_exists(new_client_dict, 'nifi.registry.security.truststore')):
        return True
    elif orig_client_dict['nifi.registry.security.keystoreType'] != new_client_dict['nifi.registry.security.keystoreType']:
        return True
    elif ('#nifi.registry.security.ambari.hash.kspwd' not in orig_client_dict) or not match(orig_client_dict['#nifi.registry.security.ambari.hash.kspwd'], hash(new_client_dict['nifi.registry.security.keystorePasswd'], master_key)):
        return True
    elif ('#nifi.registry.security.ambari.hash.kpwd' not in orig_client_dict) or not match(orig_client_dict['#nifi.registry.security.ambari.hash.kpwd'], hash(new_client_dict['nifi.registry.security.keyPasswd'], master_key)):
        return True
    elif orig_client_dict['nifi.registry.security.truststoreType'] != new_client_dict['nifi.registry.security.truststoreType']:
        return True
    elif ('#nifi.registry.security.ambari.hash.tspwd' not in orig_client_dict) or not match(orig_client_dict['#nifi.registry.security.ambari.hash.tspwd'], hash(new_client_dict['nifi.registry.security.truststorePasswd'], master_key)):
        return True
    elif orig_client_dict['nifi.registry.security.keystore'] != new_client_dict['nifi.registry.security.keystore']:
        return True
    elif orig_client_dict['nifi.registry.security.truststore'] != new_client_dict['nifi.registry.security.truststore']:
        return True
    else:
        return False

def move_keystore_truststore(client_dict):
    move_store(client_dict, 'nifi.registry.security.keystore')
    move_store(client_dict, 'nifi.registry.security.truststore')

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

        newKeyPasswd = new_prop['nifi.registry.security.keyPasswd'].replace('{{nifi_registry_keyPasswd}}',params.nifi_registry_keyPasswd)
        newKeystorePasswd = new_prop['nifi.registry.security.keystorePasswd'].replace('{{nifi_registry_keystorePasswd}}',params.nifi_registry_keystorePasswd)
        newTruststorePasswd = new_prop['nifi.registry.security.truststorePasswd'].replace('{{nifi_registry_truststorePasswd}}',params.nifi_registry_truststorePasswd)

        if len(newKeyPasswd) == 0 and len(old_prop['nifi.registry.security.keyPasswd']) > 0:
            new_prop['nifi.registry.security.keyPasswd'] = old_prop['nifi.registry.security.keyPasswd']
            if 'nifi.registry.security.keyPasswd.protected' in old_prop:
                new_prop['nifi.registry.security.keyPasswd.protected'] = old_prop['nifi.registry.security.keyPasswd.protected']

        if len(newKeystorePasswd) == 0 and len(old_prop['nifi.registry.security.keystorePasswd']) > 0:
            new_prop['nifi.registry.security.keystorePasswd'] = old_prop['nifi.registry.security.keystorePasswd']
            if 'nifi.registry.security.keystorePasswd.protected' in old_prop:
                new_prop['nifi.registry.security.keystorePasswd.protected'] = old_prop['nifi.registry.security.keystorePasswd.protected']

        if len(newTruststorePasswd) == 0 and len(old_prop['nifi.registry.security.truststorePasswd']) > 0 :
            new_prop['nifi.registry.security.truststorePasswd'] = old_prop['nifi.registry.security.truststorePasswd']
            if 'nifi.registry.security.truststorePasswd.protected' in old_prop:
                new_prop['nifi.registry.security.truststorePasswd.protected'] = old_prop['nifi.registry.security.truststorePasswd.protected']

    return new_prop

def get_nifi_ca_client_dict(config,params):

    if not config or len(config) == 0:
        return {}
    else:
        nifi_registry_keystore = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keystore']
        nifi_registry_keystoreType = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keystoreType']
        nifi_registry_keystorePasswd = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keystorePasswd']
        nifi_registry_keyPasswd = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.keyPasswd']
        nifi_registry_truststore = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.truststore']
        nifi_registry_truststoreType = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.truststoreType']
        nifi_registry_truststorePasswd = config['configurations']['nifi-registry-ambari-ssl-config']['nifi.registry.security.truststorePasswd']
        nifi_registry_truststore = nifi_registry_truststore.replace('{nifi_registry_ssl_host}',params.nifi_registry_host)
        nifi_registry_truststore = nifi_registry_truststore.replace('{{nifi_registry_config_dir}}',params.nifi_registry_config_dir)
        nifi_registry_keystore = nifi_registry_keystore.replace('{nifi_registry_ssl_host}',params.nifi_registry_host)
        nifi_registry_keystore = nifi_registry_keystore.replace('{{nifi_registry_config_dir}}',params.nifi_registry_config_dir)

        #default keystore/truststore type if empty
        nifi_registry_keystoreType = 'jks' if len(nifi_registry_keystoreType) == 0 else nifi_registry_keystoreType
        nifi_registry_truststoreType = 'jks' if len(nifi_registry_truststoreType) == 0 else nifi_registry_truststoreType

        nifi_ca_parent_config = config['configurations']['nifi-ambari-ssl-config']
        nifi_toolkit_tls_token = nifi_ca_parent_config['nifi.toolkit.tls.token']
        nifi_toolkit_tls_helper_days = nifi_ca_parent_config['nifi.toolkit.tls.helper.days']
        nifi_toolkit_tls_port = nifi_ca_parent_config['nifi.toolkit.tls.port']
        nifi_toolkit_dn_prefix = nifi_ca_parent_config['nifi.toolkit.dn.prefix']
        nifi_toolkit_dn_suffix = nifi_ca_parent_config['nifi.toolkit.dn.suffix']

        nifi_ca_client_config = {
            "days" : int(nifi_toolkit_tls_helper_days),
            "keyStore" : nifi_registry_keystore,
            "keyStoreType" : nifi_registry_keystoreType,
            "keyStorePassword" : nifi_registry_keystorePasswd,
            "keyPassword" : nifi_registry_keyPasswd,
            "token" : nifi_toolkit_tls_token,
            "dn" : nifi_toolkit_dn_prefix + params.nifi_registry_host + nifi_toolkit_dn_suffix,
            "port" : int(nifi_toolkit_tls_port),
            "caHostname" : params.nifi_ca_host,
            "trustStore" : nifi_registry_truststore,
            "trustStoreType" : nifi_registry_truststoreType,
            "trustStorePassword": nifi_registry_truststorePasswd
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

def create_keystore_truststore(is_starting, params):
    if is_starting:

        updated_properties = run_toolkit_client(get_nifi_ca_client_dict(params.config, params), params.nifi_registry_config_dir,
                                                params.jdk64_home, params.nifi_toolkit_java_options,
                                                params.nifi_registry_user, params.nifi_registry_group,
                                                params.toolkit_tmp_dir, params.stack_version_buildnum, params.stack_support_toolkit_update)

        update_nifi_ca_registry_properties(updated_properties, params.nifi_registry_properties)

    return params.nifi_registry_properties

@retry(times=20, sleep_time=5, max_sleep_time=20, backoff_factor=2, err_class=Fail)
def run_toolkit_client(ca_client_dict, nifi_registry_config_dir, jdk64_home, java_options, nifi_registry_user,nifi_registry_group,toolkit_tmp_dir, stack_version_buildnum, no_client_file=False):
    Logger.info("Generating NiFi Registry Keystore and Truststore")
    ca_client_script = get_toolkit_script('tls-toolkit.sh',toolkit_tmp_dir, stack_version_buildnum)
    File(ca_client_script, mode=0755)
    if no_client_file:
        Logger.info("Executing toolkit without client file")
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
            shell.call(['chown',nifi_registry_user+':'+nifi_registry_group,updated_properties['keyStore']],sudo=True)
            shell.call(['chown',nifi_registry_user+':'+nifi_registry_group,updated_properties['trustStore']],sudo=True)
    else:
        Logger.info("Executing toolkit with client file")
        ca_client_json = os.path.realpath(os.path.join(nifi_registry_config_dir, 'nifi-certificate-authority-client.json'))
        dump(ca_client_json, ca_client_dict, nifi_registry_user, nifi_registry_group)
        environment = {'JAVA_HOME': jdk64_home, 'JAVA_OPTS': java_options}
        Execute((ca_client_script, 'client', '-F', '-f', ca_client_json), user=nifi_registry_user, environment=environment)
        updated_properties = load(ca_client_json)

    return updated_properties

def clean_toolkit_client_files(old_nifi_registry_properties, new_nifi_registry_properties):
    move_keystore_truststore(old_nifi_registry_properties)
    new_nifi_registry_properties['nifi.registry.security.keystore'] = ''
    new_nifi_registry_properties['nifi.registry.security.truststore'] = ''
    return new_nifi_registry_properties

def encrypt_sensitive_properties(nifi_registry_config_dir, jdk64_home, java_options, nifi_registry_user, last_master_key, master_key_password, is_starting,toolkit_tmp_dir, stack_version_buildnum):
    Logger.info("Encrypting NiFi Registry sensitive configuration properties")
    encrypt_config_script = get_toolkit_script('encrypt-config.sh',toolkit_tmp_dir, stack_version_buildnum)
    encrypt_config_command = (encrypt_config_script,)
    environment = {'JAVA_HOME': jdk64_home, 'JAVA_OPTS': java_options}
    File(encrypt_config_script, mode=0755)

    if is_starting:

        encrypt_config_command += ('--nifiRegistry', '-v', '-b', nifi_registry_config_dir + '/bootstrap.conf')
        encrypt_config_command += ('-r', nifi_registry_config_dir + '/nifi-registry.properties')

        if contains_providers(nifi_registry_config_dir+'/identity-providers.xml', "provider"):
            encrypt_config_command += ('-i', nifi_registry_config_dir + '/identity-providers.xml')

        if contains_providers(nifi_registry_config_dir+'/authorizers.xml', "authorizer"):
            encrypt_config_command += ('-a', nifi_registry_config_dir + '/authorizers.xml')

        if last_master_key:
            encrypt_config_command += ('--oldKey', PasswordString(last_master_key))

        encrypt_config_command += ('-p', PasswordString(master_key_password))
        Execute(encrypt_config_command, user=nifi_registry_user, logoutput=False, environment=environment)

