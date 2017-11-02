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
from resource_management.libraries.functions.get_bare_principal import get_bare_principal
from ambari_server.serverConfiguration import get_ambari_properties, get_ambari_version

class HDF30StackAdvisor(HDF21StackAdvisor):

  def getServiceConfigurationRecommenderDict(self):
    parentRecommendConfDict = super(HDF30StackAdvisor, self).getServiceConfigurationRecommenderDict()
    childRecommendConfDict = {
      "RANGER": self.recommendRangerConfigurations,
      "STREAMLINE": self.recommendStreamlineConfigurations
    }
    parentRecommendConfDict.update(childRecommendConfDict)
    return parentRecommendConfDict

  def getServiceConfigurationValidators(self):
    parentValidators = super(HDF30StackAdvisor, self).getServiceConfigurationValidators()
    childValidators = {
        "RANGER": {"ranger-ugsync-site": self.validateRangerUsersyncConfigurations}
    }
    self.mergeValidators(parentValidators, childValidators)
    return parentValidators

  def recommendStreamlineConfigurations(self, configurations, clusterData, services, hosts):
    super(HDF30StackAdvisor, self).recommendRangerConfigurations(configurations, clusterData, services, hosts)
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]
    security_enabled = self.isSecurityEnabled(services)
    if 'STORM' in servicesList and security_enabled:
      storm_site = self.getServicesSiteProperties(services, "storm-site")
      if storm_site is not None:
        putStormSiteProperty = self.putProperty(configurations, "storm-site", services)
        putStormSiteAttributes = self.putPropertyAttribute(configurations, "storm-site")
        storm_env = self.getServicesSiteProperties(services, "storm-env")
        storm_nimbus_impersonation_acl = storm_site["nimbus.impersonation.acl"] if "nimbus.impersonation.acl" in storm_site else None
        streamline_env = self.getServicesSiteProperties(services, "streamline-env")
        _streamline_principal_name = streamline_env['streamline_principal_name'] if 'streamline_principal_name' in streamline_env else None
        if _streamline_principal_name is not None and storm_nimbus_impersonation_acl is not None:
          streamline_bare_principal = get_bare_principal(_streamline_principal_name)
          storm_nimbus_impersonation_acl=storm_nimbus_impersonation_acl.replace('{{streamline_bare_principal}}', streamline_bare_principal)
          putStormSiteProperty('nimbus.impersonation.acl', storm_nimbus_impersonation_acl)

        storm_nimbus_autocred_plugin_classes = storm_site["nimbus.autocredential.plugins.classes"] if "nimbus.autocredential.plugins.classes" in storm_site else None
        if storm_nimbus_autocred_plugin_classes is not None:
          new_storm_nimbus_autocred_plugin_classes = ['org.apache.storm.hdfs.security.AutoHDFS',
                                                      'org.apache.storm.hbase.security.AutoHBase',
                                                      'org.apache.storm.hive.security.AutoHive']
          new_conf = DefaultStackAdvisor.appendToYamlString(storm_nimbus_autocred_plugin_classes,
                                        new_storm_nimbus_autocred_plugin_classes)

          putStormSiteProperty("nimbus.autocredential.plugins.classes", new_conf)
        else:
          putStormSiteProperty("nimbus.autocredential.plugins.classes", "['org.apache.storm.hdfs.security.AutoHDFS', 'org.apache.storm.hbase.security.AutoHBase', 'org.apache.storm.hive.security.AutoHive']")

        storm_nimbus_credential_renewer_classes = storm_site["nimbus.credential.renewers.classes"] if "nimbus.credential.renewers.classes" in storm_site else None
        if storm_nimbus_credential_renewer_classes is not None:
          new_storm_nimbus_credential_renewer_classes_array = ['org.apache.storm.hdfs.security.AutoHDFS',
                                                               'org.apache.storm.hbase.security.AutoHBase',
                                                               'org.apache.storm.hive.security.AutoHive']
          new_conf = DefaultStackAdvisor.appendToYamlString(storm_nimbus_credential_renewer_classes,
                                        new_storm_nimbus_credential_renewer_classes_array)
          putStormSiteProperty("nimbus.autocredential.plugins.classes", new_conf)
        else:
          putStormSiteProperty("nimbus.credential.renewers.classes", "['org.apache.storm.hdfs.security.AutoHDFS', 'org.apache.storm.hbase.security.AutoHBase', 'org.apache.storm.hive.security.AutoHive']")
        putStormSiteProperty("nimbus.credential.renewers.freq.secs", "82800")

  def recommendRangerConfigurations(self, configurations, clusterData, services, hosts):
    super(HDF30StackAdvisor, self).recommendRangerConfigurations(configurations, clusterData, services, hosts)

    putRangerUgsyncSite = self.putProperty(configurations, 'ranger-ugsync-site', services)

    delta_sync_enabled = False
    if 'ranger-ugsync-site' in services['configurations'] and 'ranger.usersync.ldap.deltasync' in services['configurations']['ranger-ugsync-site']['properties']:
      delta_sync_enabled = services['configurations']['ranger-ugsync-site']['properties']['ranger.usersync.ldap.deltasync'] == "true"

    if delta_sync_enabled:
      putRangerUgsyncSite("ranger.usersync.group.searchenabled", "true")
    else:
      putRangerUgsyncSite("ranger.usersync.group.searchenabled", "false")

  def validateRangerUsersyncConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    ranger_usersync_properties = properties
    validationItems = []

    delta_sync_enabled = 'ranger.usersync.ldap.deltasync' in ranger_usersync_properties \
      and ranger_usersync_properties['ranger.usersync.ldap.deltasync'].lower() == 'true'
    group_sync_enabled = 'ranger.usersync.group.searchenabled' in ranger_usersync_properties \
      and ranger_usersync_properties['ranger.usersync.group.searchenabled'].lower() == 'true'

    if delta_sync_enabled and not group_sync_enabled:
      validationItems.append({"config-name": "ranger.usersync.group.searchenabled",
                            "item": self.getWarnItem(
                            "Need to set ranger.usersync.group.searchenabled as true, as ranger.usersync.ldap.deltasync is enabled")})

    return self.toConfigurationValidationProblems(validationItems, "ranger-ugsync-site")

  def getCardinalitiesDict(self, hosts):
    return {
      'ZOOKEEPER_SERVER': {"min": 3},
      'METRICS_COLLECTOR': {"min": 1}
    }
