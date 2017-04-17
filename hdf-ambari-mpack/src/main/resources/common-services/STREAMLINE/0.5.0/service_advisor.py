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


class StreamlineServiceAdvisor(service_advisor.ServiceAdvisor):

  def __init__(self, *args, **kwargs):
    self.as_super = super(StreamlineServiceAdvisor, self)
    self.as_super.__init__(*args, **kwargs)

  def getServiceConfigurationRecommenderDict(self):
    """
    Recommend configurations to set. Streamline does not have any recommendations in this version.
    """
    Logger.info("Class: %s, Method: %s. Recommending Service Configurations." %
                (self.__class__.__name__, inspect.stack()[0][3]))

    return self.as_super.getServiceConfigurationRecommenderDict()

  def getServiceConfigurationValidators(self):
    """
    Get a list of errors. Streamline does not have any validations in this version.
    """
    Logger.info("Class: %s, Method: %s. Validating Service Component Layout." %
                (self.__class__.__name__, inspect.stack()[0][3]))
    return self.as_super.getServiceConfigurationValidators()


  def recommendConfigurations(self, configurations, clusterData, services, hosts):
    """
    Recommend configurations for this service.
    """
    Logger.info("Class: %s, Method: %s. Recommending Service Configurations." %
                (self.__class__.__name__, inspect.stack()[0][3]))

    pass

  def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
    """
    Validate configurations for the service. Return a list of errors.
    """
    Logger.info("Class: %s, Method: %s. Validating Configurations." %
                (self.__class__.__name__, inspect.stack()[0][3]))

    items = []

    return items

  def getCardinalitiesDict(self, hosts):
      return {'STREAMLINE_SERVER': {"min": 1}}
