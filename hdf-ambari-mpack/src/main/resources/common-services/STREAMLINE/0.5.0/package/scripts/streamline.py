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
import collections
import os

from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.resources.properties_file import PropertiesFile
from resource_management.libraries.resources.template_config import TemplateConfig
from resource_management.core.resources.system import Directory, Execute, File, Link
from resource_management.core.source import StaticFile, Template, InlineTemplate, DownloadSource
from resource_management.libraries.functions import format
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions import Direction

from resource_management.core.logger import Logger

def streamline(env, upgrade_type=None):
    import params
    ensure_base_directories()
    #Logger.info(format("Effective stack version: {effective_version}"))

    File(format("{conf_dir}/streamline-env.sh"),
          owner=params.streamline_user,
          content=InlineTemplate(params.streamline_env_sh_template)
     )

    # On some OS this folder could be not exists, so we will create it before pushing there files
    Directory(params.limits_conf_dir,
              create_parents = True,
              owner='root',
              group='root'
    )

    Directory([params.jar_storage],
            owner=params.streamline_user,
            group=params.user_group,
            create_parents = True,
            cd_access="a",
            mode=0755,
    )

    File(os.path.join(params.limits_conf_dir, 'streamline.conf'),
         owner='root',
         group='root',
         mode=0644,
         content=Template("streamline.conf.j2")
    )
    
    File(format("{conf_dir}/streamline.yaml"),
         content=Template("streamline.yaml.j2"),
         owner=params.streamline_user,
         group=params.user_group,
         mode=0644
    )

    if not os.path.islink(params.streamline_managed_log_dir):
      Link(params.streamline_managed_log_dir,
           to=params.streamline_log_dir)

    download_database_connector_if_needed()


def ensure_base_directories():
  import params
  import status_params
  Directory([params.streamline_log_dir, status_params.streamline_pid_dir, params.conf_dir, params.streamline_agent_dir],
            mode=0755,
            cd_access='a',
            owner=params.streamline_user,
            group=params.user_group,
            create_parents = True,
            recursive_ownership = True,
            )


def download_database_connector_if_needed():
  """
  Downloads the database connector to use when connecting to the metadata storage
  """
  import params
  if params.streamline_storage_type != 'mysql':
      return

  print "hello"
  print params.jdbc_driver_jar
  if params.jdbc_driver_jar == None:
      Logger.warn("Failed to find mysql-java-connector jar. Make sure you followed the steps to register mysql driver")
      show_logs(params.streamline_log_dir, params.streamline_user)

  File(params.check_db_connection_jar,
       content = DownloadSource(format("{jdk_location}{check_db_connection_jar_name}")))

  target_jar_with_directory = params.connector_download_dir + os.path.sep + params.jdbc_driver_jar
  target_jar_bootstrap_dir = params.connector_bootstrap_download_dir + os.path.sep + params.jdbc_driver_jar

  if not os.path.exists(target_jar_with_directory):
      File(params.downloaded_custom_connector,
           content=DownloadSource(params.connector_curl_source))

      Execute(('cp', '--remove-destination', params.downloaded_custom_connector, target_jar_with_directory),
              path=["/bin", "/usr/bin/"],
              sudo=True)

      File(target_jar_with_directory, owner="root",
           group=params.user_group)

  if not os.path.exists(target_jar_bootstrap_dir):
      File(params.downloaded_custom_connector,
         content=DownloadSource(params.connector_curl_source))

      Execute(('cp', '--remove-destination', params.downloaded_custom_connector, target_jar_bootstrap_dir),
              path=["/bin", "/usr/bin/"],
              sudo=True)

      File(target_jar_with_directory, owner="root",
           group=params.user_group)
