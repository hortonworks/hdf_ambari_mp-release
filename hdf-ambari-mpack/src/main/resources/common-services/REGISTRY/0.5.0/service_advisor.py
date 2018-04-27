#!/usr/bin/env ambari-python-wrap
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
import imp
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_DIR = os.path.join(SCRIPT_DIR, '../0.3.0/')
PARENT_FILE = os.path.join(SERVICE_DIR, 'service_advisor.py')

try:
    with open(PARENT_FILE, 'rb') as fp:
        service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
    traceback.print_exc()
    print "Failed to load parent"

DB_TYPE_DEFAULT_PORT_MAP = {"mysql":"3306", "oracle":"1521", "postgresql":"5432"}

class REGISTRY050ServiceAdvisor(service_advisor.REGISTRY030ServiceAdvisor):

  def autopopulateREGISTRYJdbcUrl(self, configurations, services):

    putRegistryCommonProperty = self.putProperty(configurations, "registry-common", services)

    registry_storage_database = services['configurations']['registry-common']['properties']['database_name']
    registry_storage_type = str(services['configurations']['registry-common']['properties']['registry.storage.type']).lower()
    registry_storage_connector_connectURI = services['configurations']['registry-common']['properties']['registry.storage.connector.connectURI']

    if "oracle" in registry_storage_connector_connectURI:
      registry_db_hostname = registry_storage_connector_connectURI.split(":")[3].strip("@")
    else:
      registry_db_hostname = registry_storage_connector_connectURI.split(":")[2].strip("/")

    registry_db_url_dict = {
      'mysql': {'registry.storage.connector.connectURI': 'jdbc:mysql://' + registry_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[registry_storage_type] + '/' + registry_storage_database},
      'oracle': {'registry.storage.connector.connectURI': 'jdbc:oracle:thin:@' + registry_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[registry_storage_type] + '/' + registry_storage_database},
      'postgresql': {'registry.storage.connector.connectURI': 'jdbc:postgresql://' + registry_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[registry_storage_type] + '/' + registry_storage_database},
    }

    registryDbProperties = registry_db_url_dict.get(registry_storage_type, registry_db_url_dict['mysql'])
    for key in registryDbProperties:
      putRegistryCommonProperty(key, registryDbProperties.get(key))

    db_root_jdbc_url_dict = {
      'mysql': {'db_root_jdbc_url': 'jdbc:mysql://' + registry_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[registry_storage_type]},
      'postgresql': {'db_root_jdbc_url': 'jdbc:postgresql://' + registry_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[registry_storage_type]},
      }

    registryPrivelegeDbProperties = db_root_jdbc_url_dict.get(registry_storage_type, db_root_jdbc_url_dict['mysql'])
    for key in registryPrivelegeDbProperties:
      putRegistryCommonProperty(key, registryPrivelegeDbProperties.get(key))

  def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
    super(REGISTRY050ServiceAdvisor, self).getServiceConfigurationRecommendations(configurations, clusterData, services, hosts)
    self.autopopulateREGISTRYJdbcUrl(configurations, services)

