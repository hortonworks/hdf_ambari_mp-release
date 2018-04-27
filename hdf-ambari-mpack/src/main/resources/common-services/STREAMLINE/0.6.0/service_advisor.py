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
SERVICE_DIR = os.path.join(SCRIPT_DIR, '../0.5.0/')
PARENT_FILE = os.path.join(SERVICE_DIR, 'service_advisor.py')

try:
    with open(PARENT_FILE, 'rb') as fp:
        service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
    traceback.print_exc()
    print "Failed to load parent"

DB_TYPE_DEFAULT_PORT_MAP = {"mysql":"3306", "oracle":"1521", "postgresql":"5432"}

class STREAMLINE060ServiceAdvisor(service_advisor.STREAMLINE050ServiceAdvisor):

  def autopopulateSTREAMLINEJdbcUrl(self, configurations, services):

    putStreamlineCommonProperty = self.putProperty(configurations, "streamline-common", services)

    streamline_storage_database = services['configurations']['streamline-common']['properties']['database_name']
    streamline_storage_type = str(services['configurations']['streamline-common']['properties']['streamline.storage.type']).lower()
    streamline_storage_connector_connectURI = services['configurations']['streamline-common']['properties']['streamline.storage.connector.connectURI']

    if "oracle" in streamline_storage_connector_connectURI:
      streamline_db_hostname = streamline_storage_connector_connectURI.split(":")[3].strip("@")
    else:
      streamline_db_hostname = streamline_storage_connector_connectURI.split(":")[2].strip("/")

    streamline_db_url_dict = {
      'mysql': {'streamline.storage.connector.connectURI': 'jdbc:mysql://' + streamline_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[streamline_storage_type] + '/' + streamline_storage_database},
      'oracle': {'streamline.storage.connector.connectURI': 'jdbc:oracle:thin:@' + streamline_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[streamline_storage_type] + '/' + streamline_storage_database},
      'postgresql': {'streamline.storage.connector.connectURI': 'jdbc:postgresql://' + streamline_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[streamline_storage_type] + '/' + streamline_storage_database},
      }

    streamlineDbProperties = streamline_db_url_dict.get(streamline_storage_type, streamline_db_url_dict['mysql'])
    for key in streamlineDbProperties:
      putStreamlineCommonProperty(key, streamlineDbProperties.get(key))

    db_root_jdbc_url_dict = {
      'mysql': {'db_root_jdbc_url': 'jdbc:mysql://' + streamline_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[streamline_storage_type]},
      'postgresql': {'db_root_jdbc_url': 'jdbc:postgresql://' + streamline_db_hostname + ':' + DB_TYPE_DEFAULT_PORT_MAP[streamline_storage_type]},
      }

    streamlinePrivelegeDbProperties = db_root_jdbc_url_dict.get(streamline_storage_type, db_root_jdbc_url_dict['mysql'])
    for key in streamlinePrivelegeDbProperties:
      putStreamlineCommonProperty(key, streamlinePrivelegeDbProperties.get(key))

  def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
    super(STREAMLINE060ServiceAdvisor, self).getServiceConfigurationRecommendations(configurations, clusterData, services, hosts)
    self.autopopulateSTREAMLINEJdbcUrl(configurations, services)