#!/usr/bin/env ambari-python-wrap
"""
Copyright (c) 2011-2018, Hortonworks Inc.  All rights reserved.
Except as expressly permitted in a written agreement between you
or your company and Hortonworks, Inc, any use, reproduction,
modification,
redistribution, sharing, lending or other exploitation
of all or any part of the contents of this file is strictly prohibited.
"""
import os
import imp
import traceback
from urlparse import urlparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../../')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"


class HDP21SMARTSENSEServiceAdvisor(service_advisor.ServiceAdvisor):
    NON_ACTIVITY_SMARTSENSE_COMPONENTS = ['HST_SERVER', 'HST_AGENT', 'ACTIVITY_EXPLORER', 'ACTIVITY_ANALYZER']

    def __init__(self, *args, **kwargs):
        self.as_super = super(HDP21SMARTSENSEServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)

    def colocateService(self, hostsComponentsMap, serviceComponents):
        analyzerComponents = [component for component in serviceComponents \
            if component["StackServiceComponents"]["component_name"] not in HDP21SMARTSENSEServiceAdvisor.NON_ACTIVITY_SMARTSENSE_COMPONENTS]
        placeExplorer = False
        for component in analyzerComponents:
            if self.isComponentHostsPopulated(component):
                continue

            componentName = component["StackServiceComponents"]["component_name"]
            if not self.checkComponentDependenciesExists(component, hostsComponentsMap):
                # If one or more dependencies does not exists, assume the service is not available.
                # We don't automatically place component if service availability can't be determined.
                hosts = []
            elif componentName == 'HDFS_ANALYZER':
                hosts = self.findHostsRunningComponents(['NAMENODE'], hostsComponentsMap)
            else:
                associatedComponents = ['HST_SERVER', 'ACTIVITY_EXPLORER', 'AMBARI_SERVER', 'METRICS_COLLECTOR']
                hosts = self.findHostsRunningComponents(associatedComponents, hostsComponentsMap)

                # Exclude NameNode hosts
                nn_hosts = self.findHostsRunningComponents(['NAMENODE'], hostsComponentsMap)
                filtered_hosts = list(set(hosts) - set(nn_hosts))
                if filtered_hosts:
                    hosts = filtered_hosts

            placeExplorer = placeExplorer or len(hosts) > 0
            # placing component on hosts
            self.placeComponentOnHosts(componentName, hosts, hostsComponentsMap, serviceComponents)

        if placeExplorer == True:
            explorerComponents = [component for component in serviceComponents \
                if component["StackServiceComponents"]["component_name"] == 'ACTIVITY_EXPLORER']
            if len(explorerComponents) > 0 and not self.isComponentHostsPopulated(explorerComponents[0]):
                # Colocate ACTIVITY_EXPLORER in HST_SERVER host
                hstServerHosts = self.findHostsRunningComponents(['HST_SERVER'], hostsComponentsMap)
                self.placeComponentOnHosts('ACTIVITY_EXPLORER', hstServerHosts[:1], hostsComponentsMap, serviceComponents)


        # Remove ACTIVITY_ANALYZER from all hosts and in the deployment
        for hostName in hostsComponentsMap.keys():
            hostComponents = hostsComponentsMap[hostName]
            if {"name": "ACTIVITY_ANALYZER"} in hostComponents:
                hostComponents.remove({"name": "ACTIVITY_ANALYZER"})
        for component in serviceComponents:
            if component["StackServiceComponents"]["component_name"]  != "ACTIVITY_ANALYZER":
                continue
            serviceComponents.remove(component)
            break

    def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):

        # Automatically generate the hst server url in the form http(s)://<hst server host>:<hst ui port>
        protocol = 'http'
        port = 9000
        hst_server_hostname = 'localhost'

        if 'hst-server-conf' in services['configurations'] and 'properties' in services['configurations']['hst-server-conf']:
            if 'server.ssl_enabled' in services['configurations']['hst-server-conf']['properties'] \
                and (services['configurations']['hst-server-conf']['properties']['server.ssl_enabled'] == True \
                      or str(services['configurations']['hst-server-conf']['properties']['server.ssl_enabled']).lower()) == 'true':
                    protocol = "https"

            if 'server.port' in services['configurations']['hst-server-conf']['properties']:
                    port = services['configurations']['hst-server-conf']['properties']['server.port']

            hst_server_hostnames = self.getComponentHostNames(services, "SMARTSENSE", "HST_SERVER")
            if hst_server_hostnames :
                hst_server_hostname = hst_server_hostnames[0]
            print "Ambari returned '%s' as HST server hostname." % hst_server_hostname
            if hst_server_hostname == 'localhost' and 'server.url' in services['configurations']['hst-server-conf']['properties']:
                server_url = services['configurations']['hst-server-conf']['properties']['server.url']
                prev_hst_server_hostname = urlparse(server_url).hostname if server_url and server_url.startswith('http') else ''
                if len(prev_hst_server_hostname.strip()) > 0:
                    print "Setting previous set server.url host '%s' as HST server hostname." % prev_hst_server_hostname
                    hst_server_hostname = prev_hst_server_hostname

        putHstServerConfProperty = self.putProperty(configurations, "hst-server-conf", services)
        putHstServerConfProperty('server.url', protocol + "://" + hst_server_hostname + ":" + str(port))


        # if self.isSecurityEnabled(services): ## Commenting out as we should have these setup even for non kerberos clusters
        # Get activity-conf/global.activity.analyzer.user and activity-conf/activity.explorer.user, if available
        if 'activity-conf' in services['configurations'] and 'properties' in services['configurations']['activity-conf']:
            global_activity_analyzer_user = services['configurations']['activity-conf']['properties']['global.activity.analyzer.user'] \
                if 'global.activity.analyzer.user' in services['configurations']['activity-conf']['properties'] \
                else None

            activity_explorer_user = services['configurations']['activity-conf']['properties']['activity.explorer.user'] \
                if 'activity.explorer.user' in services['configurations']['activity-conf']['properties'] \
                else None
        else:
            global_activity_analyzer_user = None
            activity_explorer_user = None

        # If activity-conf/global.activity.analyzer.user is available, append it to the set of users
        # listed in yarn-site/yarn.admin.acl
        if global_activity_analyzer_user is not None and global_activity_analyzer_user != '':
          if ('yarn-site' in services['configurations']) and ('properties' in services['configurations']['yarn-site']):
            yarn_site_properties = services["configurations"]["yarn-site"]["properties"]

            if 'yarn-site' in configurations and 'properties' in configurations['yarn-site'] \
              and 'yarn.admin.acl' in configurations['yarn-site']['properties']:
              yarn_admin_acl = configurations['yarn-site']['properties']['yarn.admin.acl']
            elif 'yarn.admin.acl' in yarn_site_properties:
              yarn_admin_acl = yarn_site_properties['yarn.admin.acl']
            else:
              yarn_admin_acl = None

            # Create a unique set of user names for the new yarn.admin.acl
            user_names = set()
            user_names.add(global_activity_analyzer_user)

            if yarn_admin_acl is not None and yarn_admin_acl != '':
              # Parse yarn_admin_acl to get a set of unique user names
              for user_name in yarn_admin_acl.split(','):
                user_name = user_name.strip()
                if user_name:
                  user_names.add(user_name)

            yarn_admin_acl = ','.join(user_names)

            putYarnSiteProperty = self.putProperty(configurations, "yarn-site", services)
            putYarnSiteProperty('yarn.admin.acl', yarn_admin_acl)


        # If activity-conf/global.activity.analyzer.user or activity-conf/activity.explorer.user are
        # available, append them to the set of users listed in ams-hbase-site/hbase.superuser
        if (global_activity_analyzer_user is not None and global_activity_analyzer_user != '') \
          or (activity_explorer_user is not None and activity_explorer_user != ''):

          if ('ams-hbase-site' in services['configurations']) and ('properties' in services['configurations']['ams-hbase-site']):
            ams_hbase_site_properties = services["configurations"]["ams-hbase-site"]["properties"]

            if 'ams-hbase-site' in configurations and 'properties' in configurations['ams-hbase-site'] \
              and 'hbase.superuser' in configurations['ams-hbase-site']['properties']:
              hbase_superuser = configurations['ams-hbase-site']['properties']['hbase.superuse']
            elif 'hbase.superuser' in ams_hbase_site_properties:
              hbase_superuser = ams_hbase_site_properties['hbase.superuser']
            else:
              hbase_superuser = None

            # Create a unique set of user names for the new hbase.superuser value
            user_names = set()
            if global_activity_analyzer_user is not None and global_activity_analyzer_user != '':
              user_names.add(global_activity_analyzer_user)

            if activity_explorer_user is not None and activity_explorer_user != '':
              user_names.add(activity_explorer_user)

            # Parse hbase_superuser to get a set of unique user names
            if hbase_superuser is not None and hbase_superuser != '':
              for user_name in hbase_superuser.split(','):
                user_name = user_name.strip()
                if user_name:
                  user_names.add(user_name)

            hbase_superuser = ','.join(user_names)

            putAmsHbaseSiteProperty = self.putProperty(configurations, "ams-hbase-site", services)
            putAmsHbaseSiteProperty('hbase.superuser', hbase_superuser)

    def getServiceComponentLayoutValidations(self, services, hosts):
        componentsListList = [service["components"] for service in services["services"]]
        componentsList = [item["StackServiceComponents"] for sublist in componentsListList for item in sublist]

        items = []

        # Make sure HDFS_ANALYZER are deployed on NAMENODE hosts, otherwise warn
        hdfsHosts = self.getHosts(componentsList, "HDFS_ANALYZER")
        if hdfsHosts and self.findService(services, "HDFS"):
            namenodeHosts = self.getHosts(componentsList, "NAMENODE")
            nonNamenodeHdfsAnalyzerHosts = set(hdfsHosts) - set(namenodeHosts)
            if len(nonNamenodeHdfsAnalyzerHosts) > 0:
                # There are some HDFS analyzers which are not placed in NameNodes
                message = "HDFS Analyzer being installed on non-namenode host(s) {0}.  It is strongly recommended " \
                    "to colocate HDFS Analyzer(s) on NameNode host(s) {1}".format(
                        ', '.join(nonNamenodeHdfsAnalyzerHosts),
                        ', '.join(set(namenodeHosts) - set(hdfsHosts))
                    )
                items.append( { "type": "host-component", "level": "ERROR", "message": message, "component-name": "HDFS_ANALYZER" })

        # Make sure analyzer components have their dependencies also installed
        serviceComponents = self.getServiceComponents(services, 'SMARTSENSE')
        for component in serviceComponents:
            componentName = component['StackServiceComponents']['component_name']
            componentDisplayName = component['StackServiceComponents']['display_name'] \
                if 'display_name' in component['StackServiceComponents'] and component['StackServiceComponents']['display_name'] else componentName
            if componentName in HDP21SMARTSENSEServiceAdvisor.NON_ACTIVITY_SMARTSENSE_COMPONENTS:
                # No need to place the non-activity smartsense components
                continue
            componentHosts = self.getHosts(componentsList, componentName)
            if not componentHosts:
                # This component is not getting installed on any hosts
                continue
            serviceName = componentName.split('_')[0]
            if serviceName == 'MAPREDUCE':
                serviceName = 'MAPREDUCE2'
            #check service is available
            if self.findService(services, serviceName) is None:
                message = "{0} service not available in this cluster and installing component {1} might fail.  " \
                    "It is strongly recommended not to select this component.".format(serviceName, componentDisplayName)
                items.append( { "type": "host-component", "level": "ERROR", "message": message, "component-name": componentName })
                continue
            componentDependencies = component['dependencies'] if 'dependencies' in component else []
            for dependency in componentDependencies:
                dependencyComponentName = dependency["Dependencies"]["component_name"]
                dependentComponent = next((c for c in componentsList if c["component_name"] == dependencyComponentName), None)
                if not dependentComponent:
                    message = "Dependent component {0} of component {1} not available in this cluster and " \
                        " installing this component might fail. It is recommended not to select this " \
                        "component.".format(dependencyComponentName, componentDisplayName)
                    items.append( { "type": 'host-component', "level": 'ERROR', "message": message })
                    break

        # Activity Explorer should be selected only if any ANALYZER is selected
        explorerHosts = self.getHosts(componentsList, 'ACTIVITY_EXPLORER')
        if len(explorerHosts) > 0:
            anyAnalyzerSelected = False
            for component in serviceComponents:
                componentName = component['StackServiceComponents']['component_name']
                if componentName in HDP21SMARTSENSEServiceAdvisor.NON_ACTIVITY_SMARTSENSE_COMPONENTS:
                    continue
                if len(self.getHosts(componentsList, componentName)) > 0:
                    anyAnalyzerSelected = True
                    break
            if anyAnalyzerSelected == False:
                message = "Activity explorer is required only if one or more activity analyzer(s) available. " \
                    "It is recommended not to select Activity Explorer component."
                items.append( { "type": "host-component", "level": "WARN", "message": message, "component-name": "ACTIVITY_EXPLORER" })
        return items

    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        return []

    def placeComponentOnHosts(self, componentName, hosts, hostsComponentsMap, serviceComponents):
        if hosts is None:
            # Invalid hosts list to place component
            return

        # Remove component from hosts which does not require this component to be placed
        self.removeComponentFromHostsNotInList(componentName, hosts, hostsComponentsMap)

        for hostName in hosts:
            hostComponents = hostsComponentsMap[hostName]
            if {"name": componentName} not in hostComponents:
                # Not yet placed, placing the component on this host
                hostComponents.append({"name": componentName})

            # Make sure dependentComponents also placed on the host
            dependentComponents = self.findComponentDependencies(componentName, serviceComponents)
            for dep in dependentComponents:
                if {"name": dep} not in hostComponents:
                    hostComponents.append({"name": dep})

    def removeComponentFromHostsNotInList(self, componentName, hosts, hostsComponentsMap):
        for hostName in hostsComponentsMap.keys():
            if not hostName in hosts:
                # Remove component from this host
                hostComponents = hostsComponentsMap[hostName]
                if {"name": componentName} in hostComponents:
                    hostComponents.remove({"name": componentName})
                    # TODO: May be we need to take out dependent components if no other components
                    # dependent on it.

    def findComponentDependencies(self, componentName, serviceComponents):
        dependencies = []
        for component in serviceComponents:
            if component['StackServiceComponents']['component_name'] != componentName:
                continue
            if not 'dependencies' in component:
                continue
            for dependency in component['dependencies']:
                dependencies.append(dependency['Dependencies']['component_name'])
        return dependencies

    def findHostsRunningComponents(self, componentNames, hostsComponentsMap):
        hosts = []
        for hostName in hostsComponentsMap.keys():
            hostComponents = hostsComponentsMap[hostName]
            for componentName in componentNames:
                if not {"name": componentName } in hostComponents:
                    continue
                hosts.append(hostName)
        return list(set(hosts))

    def findService(self, services, serviceName):
        for service in services["services"]:
            if service["StackServices"]["service_name"] == serviceName:
                return service
        return None

    def checkComponentDependenciesExists(self, component, hostsComponentsMap):
        componentDependencies = component['dependencies'] if 'dependencies' in component else []
        dependencies = [ dependency["Dependencies"]["component_name"] for dependency in componentDependencies ]
        if not dependencies:
            # No dependencies for this component
            return True
        dependenciesFound = []
        for host, components in hostsComponentsMap.iteritems():
            for component in components:
                if component['name'] in dependencies:
                    dependenciesFound.append(component['name'])
        return len(set(dependencies) - set(dependenciesFound)) == 0

class HDP30SMARTSENSEServiceAdvisor(HDP21SMARTSENSEServiceAdvisor):
    def __init__(self, *args, **kwargs):
        self.as_super = super(HDP30SMARTSENSEServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)

class HDF32SMARTSENSEServiceAdvisor(HDP21SMARTSENSEServiceAdvisor):
    def __init__(self, *args, **kwargs):
        self.as_super = super(HDF32SMARTSENSEServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)

class HDF33SMARTSENSEServiceAdvisor(HDP21SMARTSENSEServiceAdvisor):
    def __init__(self, *args, **kwargs):
        self.as_super = super(HDF33SMARTSENSEServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)

class SMARTSENSE27100ServiceAdvisor(HDF33SMARTSENSEServiceAdvisor):
    def __init__(self, *args, **kwargs):
        self.as_super = super(SMARTSENSE27100ServiceAdvisor, self)
        self.as_super.__init__(*args, **kwargs)