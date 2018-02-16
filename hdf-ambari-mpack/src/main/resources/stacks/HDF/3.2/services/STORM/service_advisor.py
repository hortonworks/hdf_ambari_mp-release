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
from os.path import dirname
from ambari_server.serverConfiguration import get_ambari_properties, get_ambari_version

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_DIR = os.path.join(SCRIPT_DIR, '../../../3.1/services/STORM')
PARENT_FILE = os.path.join(SERVICE_DIR, 'service_advisor.py')

try:
    with open(PARENT_FILE, 'rb') as fp:
        service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
    traceback.print_exc()
    print "Failed to load parent"

class HDF32STORMServiceAdvisor(service_advisor.HDF31STORMServiceAdvisor):

    def __init__(self, *args, **kwargs):
        self.as_super = super(HDF32STORMServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)

    def colocateService(self, hostsComponentsMap, serviceComponents):
        pass

    def getSiteProperties(self,configurations, siteName):
        siteConfig = configurations.get(siteName)
        if siteConfig is None:
            return None
        return siteConfig.get("properties")

    def getServicesSiteProperties(self,services, siteName):
        configurations = services.get("configurations")
        if not configurations:
            return None
        siteConfig = configurations.get(siteName)
        if siteConfig is None:
            return None
        return siteConfig.get("properties")

    """
    Returns an array of Validation objects about issues with the hostnames to which components are assigned.
    This should detect validation issues which are different than those the stack_advisor.py detects.
    The default validations are in stack_advisor.py getComponentLayoutValidations function.
    """

    def getServiceComponentLayoutValidations(self, services, hosts):
        items = super(HDF32STORMServiceAdvisor, self).getServiceComponentLayoutValidations(services, hosts)
        return items

    """
    Any configuration recommendations for the service should be defined in this function.
    This should be similar to any of the recommendXXXXConfigurations functions in the stack_advisor.py
    such as recommendYARNConfigurations().
    """

    def getServiceConfigurationRecommendations(self, configurations, clusterSummary, services, hosts):
        pass

    def validateStormConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
        super(HDF32STORMServiceAdvisor, self).validateStormConfigurations(properties, recommendedDefaults, configurations,
                                                                   services, hosts)
        validationItems = []
        return self.toConfigurationValidationProblems(validationItems, "storm-site")

    def validateConfigurationsForSite(self, configurations, recommendedDefaults, services, hosts, siteName, method):
        properties = self.getSiteProperties(configurations, siteName)
        if properties:
            return super(HDF32STORMServiceAdvisor, self).validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)
        else:
            return []

    """
    Any configuration validations for the service should be defined in this function.
    This should be similar to any of the validateXXXXConfigurations functions in the stack_advisor.py
    such as validateHDFSConfigurations.
    """

    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        items = []
        #siteName = "storm-site"
        #method = self.validateStormConfigurations
        #items = self.validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)
        return items





