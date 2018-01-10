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

# Python imports
import imp
import os
import traceback
import inspect
from os.path import dirname
from ambari_server.serverConfiguration import get_ambari_properties, get_ambari_version

# Local imports
from resource_management.core.logger import Logger

SCRIPT_DIR = dirname(os.path.abspath(__file__))
RESOURCES_DIR = dirname(dirname(dirname(SCRIPT_DIR)))
STACKS_DIR = os.path.join(RESOURCES_DIR, 'stacks')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
    with open(PARENT_FILE, 'rb') as fp:
        service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
    traceback.print_exc()
    print "Failed to load parent"


class NIFI_REGISTRY010ServiceAdvisor(service_advisor.ServiceAdvisor):

    def __init__(self, *args, **kwargs):
        self.as_super = super(NIFI_REGISTRY010ServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)
        Logger.initialize_logger()

    def getServiceConfigurationRecommenderDict(self):
        """
        Recommend configurations to set. NiFi Registry does not have any recommendations in this version.
        """
        Logger.info("Class: %s, Method: %s. Recommending Service Configurations." % (self.__class__.__name__, inspect.stack()[0][3]))
        return self.as_super.getServiceConfigurationRecommenderDict()

    def getServiceConfigurationValidators(self):
        """
        Get a list of errors. NiFi Registry does not have any validations in this version.
        """
        Logger.info("Class: %s, Method: %s. Validating Service Component Layout." % (self.__class__.__name__, inspect.stack()[0][3]))
        return self.as_super.getServiceConfigurationValidators()

    def recommendConfigurations(self, configurations, clusterData, services, hosts):
        """
        Recommend configurations for this service.
        """
        Logger.info("Class: %s, Method: %s. Recommending Service Configurations." % (self.__class__.__name__, inspect.stack()[0][3]))
        pass

    def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
        Logger.info("Class: %s, Method: %s. get Service Configurations Recommendations. " % (self.__class__.__name__, inspect.stack()[0][3]))


    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        Logger.info("Class: %s, Method: %s. Validating Service Configuration Items." % (self.__class__.__name__, inspect.stack()[0][3]))

        siteName = "nifi-registry-ambari-ssl-config"
        method = self.validateNiFiRegistrySslProperties
        items = self.validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)

        siteName = "nifi-registry-ambari-config"
        method = self.validateNiFiRegistryAmbariConfigurations
        items.extend(self.validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method))

        return items

    def getCardinalitiesDict(self, hosts):
        return {'NIFI_REGISTRY_MASTER': {"min": 1}}

    def putPropertyAttribute(self, config, configType):
        if configType not in config:
            config[configType] = {}

        def appendPropertyAttribute(key, attribute, attributeValue):
            if "property_attributes" not in config[configType]:
                if "property_attributes" not in config[configType]:
                    config[configType]["property_attributes"] = {}
            if key not in config[configType]["property_attributes"]:
                config[configType]["property_attributes"][key] = {}
            config[configType]["property_attributes"][key][attribute] = attributeValue if isinstance(attributeValue,
                                                                                                     list) else str(
                attributeValue)

        return appendPropertyAttribute

    def validateConfigurationsForSite(self, configurations, recommendedDefaults, services, hosts, siteName, method):
        properties = self.getSiteProperties(configurations, siteName)
        if properties:
            if siteName == 'nifi-registry-ambari-ssl-config' or siteName == 'nifi-registry-ambari-config':
                return method(properties, None, configurations, services, hosts)
            else:
                return super(NIFI_REGISTRY010ServiceAdvisor, self).validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)
        else:
            return []

    def validateNiFiRegistryAmbariConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []

        if 'nifi.registry.security.encrypt.configuration.password' in properties and len(properties['nifi.registry.security.encrypt.configuration.password']) < 12:
            validationItems.append({"config-name": 'nifi.registry.security.encrypt.configuration.password', 'item': self.getErrorItem('The password for encrypting configuration settings must be 12 or more characters.')})

        return self.toConfigurationValidationProblems(validationItems, "nifi-registry-ambari-config")


    def validateNiFiRegistrySslProperties(self, properties, recommendedDefaults, configurations, services, hosts):
        validationItems = []

        ssl_enabled = properties['nifi.registry.ssl.isenabled'] and str(properties['nifi.registry.ssl.isenabled']).lower() != 'false'
        initial_admin = properties['nifi.registry.initial.admin.identity']

        Logger.info("Validating nifi-registry-ambari-ssl-config")

        if ssl_enabled and not initial_admin:
            validationItems.append({"config-name": 'nifi.registry.initial.admin.identity', 'item': self.getWarnItem('If SSL is enabled, Initial Admin Identity should usually be configured to a DN that an admin will have a certificate for.')})

        if ssl_enabled and not self.__find_ca(services):
            if not properties['nifi.registry.security.keystorePasswd']:
                validationItems.append({"config-name": 'nifi.registry.security.keystorePasswd', 'item': self.getErrorItem('If NiFi Certificate Authority is not installed and SSL is enabled, must specify nifi.security.keystorePasswd')})
            if not properties['nifi.registry.security.keyPasswd']:
                validationItems.append({"config-name": 'nifi.registry.security.keyPasswd', 'item': self.getErrorItem('If NiFi Certificate Authority is not installed and SSL is enabled, must specify nifi.security.keyPasswd')})
            if not properties['nifi.registry.security.truststorePasswd']:
                validationItems.append({"config-name": 'nifi.registry.security.truststorePasswd', 'item': self.getErrorItem('If NiFi Certificate Authority is not installed and SSL is enabled, must specify nifi.security.truststorePasswd')})
            if not properties['nifi.registry.security.keystoreType']:
                validationItems.append({"config-name": 'nifi.registry.security.keystoreType', 'item': self.getErrorItem('If NiFi Certificate Authority is not installed and SSL is enabled, must specify nifi.security.keystoreType')})
            if not properties['nifi.registry.security.truststoreType']:
                validationItems.append({"config-name": 'nifi.registry.security.truststoreType', 'item': self.getErrorItem('If NiFi Certificate Authority is not installed and SSL is enabled, must specify nifi.security.truststoreType')})

        return self.toConfigurationValidationProblems(validationItems, "nifi-registry-ambari-ssl-config")

    def __find_ca(self, services):
        for service in services['services']:
            if 'components' in service:
                for component in service['components']:
                    stackServiceComponent = component['StackServiceComponents']
                    if 'NIFI_CA' == stackServiceComponent['component_name'] and stackServiceComponent['hostnames']:
                        return True
        return False