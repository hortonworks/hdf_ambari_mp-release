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
from resource_management import *
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import Direction
from resource_management.libraries.functions import default
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.show_logs import show_logs
import os, time
from streamline import ensure_base_directories
from streamline import streamline, wait_until_server_starts


class StreamlineServer(Script):

  def get_component_name(self):
    stack_name = default("/hostLevelParams/stack_name", None)
    if stack_name == "HDP":
      return None
    return "streamline"

  def install(self, env):
    self.install_packages(env)

  def configure(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    streamline(env, upgrade_type=None)

  def pre_upgrade_restart(self, env, upgrade_type=None):
    import params
    env.set_params(params)

  def start(self, env, upgrade_type=None):
    import params
    import status_params
    env.set_params(params)
    self.configure(env)

    if not os.path.isfile(params.bootstrap_storage_file):
        try:
          Execute(params.bootstrap_storage_run_cmd,
                  user="root")
          File(params.bootstrap_storage_file,
               owner=params.streamline_user,
               group=params.user_group,
               mode=0644)
        except:
          show_logs(params.streamline_log_dir, params.streamline_user)
          raise

    daemon_cmd = format('source {params.conf_dir}/streamline-env.sh ; {params.streamline_bin} start')
    no_op_test = format('ls {status_params.streamline_pid_file} >/dev/null 2>&1 && ps -p `cat {status_params.streamline_pid_file}` >/dev/null 2>&1')
    try:
      Execute(daemon_cmd,
              user="root",
              not_if=no_op_test
      )
    except:
      show_logs(params.streamline_log_dir, params.streamline_user)
      raise

    if not os.path.isfile(params.bootstrap_file):
      try:
        if params.security_enabled:
          kinit_cmd = format("{kinit_path_local} -kt {params.streamline_keytab_path} {params.streamline_jaas_principal};")
          return_code, out = shell.checked_call(kinit_cmd,
                                                path='/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin',
                                                user=params.streamline_user)
        wait_until_server_starts()
        Execute(params.bootstrap_run_cmd,
                user=params.streamline_user)
        File(params.bootstrap_file,
             owner=params.streamline_user,
             group=params.user_group,
             mode=0644)
      except:
        show_logs(params.streamline_log_dir, params.streamline_user)
        raise

  def stop(self, env, upgrade_type=None):
    import params
    import status_params
    env.set_params(params)
    ensure_base_directories()
    daemon_cmd = format('source {params.conf_dir}/streamline-env.sh; {params.streamline_bin} stop')
    try:
      Execute(daemon_cmd,
              user=params.streamline_user,
      )
    except:
      show_logs(params.streamline_log_dir, params.streamline_user)
      raise
    File(status_params.streamline_pid_file,
          action = "delete"
    )

  def status(self, env):
    import status_params
    check_process_status(status_params.streamline_pid_file)
    
  def get_log_folder(self):
    import params
    return params.streamline_log_dir
  
  def get_user(self):
    import params
    return params.streamline_user

  def get_pid_files(self):
    import status_params
    return [status_params.streamline_pid_file]

if __name__ == "__main__":
  StreamlineServer().execute()
