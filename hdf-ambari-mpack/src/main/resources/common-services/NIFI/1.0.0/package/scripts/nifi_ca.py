import nifi_ca_util, os, time

from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.resources.system import Directory, Execute
from resource_management.core.sudo import kill, read_file
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.script.script import Script
from signal import SIGTERM, SIGKILL

class CertificateAuthority(Script):
  def install(self, env):
    import params
    import status_params

    self.install_packages(env)

    #Be sure ca script is in cache
    nifi_ca_util.get_toolkit_script('tls-toolkit.sh')

  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)

    #create the log, pid, conf dirs if not already present
    Directory([status_params.nifi_pid_dir, params.nifi_node_log_dir, params.nifi_config_dir],
      owner=params.nifi_user,
      group=params.nifi_group,
      create_parents=True
    )

    ca_json = os.path.join(params.nifi_config_dir, 'nifi-certificate-authority.json')
    ca_dict = nifi_ca_util.load_overlay_dump(ca_json, params.nifi_ca_config)
    Directory([params.nifi_config_dir],
        owner=params.nifi_user,
        group=params.nifi_group,
        create_parents=True,
        recursive_ownership=True
    )
    
  def status(self, env):
    import status_params
    check_process_status(status_params.nifi_ca_pid_file)

  def start(self, env):
    import params
    import status_params

    self.configure(env)
    ca_server_script = nifi_ca_util.get_toolkit_script('tls-toolkit.sh')
    run_ca_script = os.path.join(os.path.dirname(__file__), 'run_ca.sh')
    Directory([params.nifi_config_dir],
        owner=params.nifi_user,
        group=params.nifi_group,
        create_parents=True,
        recursive_ownership=True
    )
    os.chmod(ca_server_script, 0755)
    os.chmod(run_ca_script, 0755)
    Execute((run_ca_script, params.jdk64_home, ca_server_script, params.nifi_config_dir + '/nifi-certificate-authority.json', params.nifi_ca_log_file_stdout, params.nifi_ca_log_file_stderr, status_params.nifi_ca_pid_file), user=params.nifi_user)
    if not os.path.isfile(status_params.nifi_ca_pid_file):
      raise Exception('Expected pid file to exist')

  def stop(self, env):
    import params
    import status_params

    try:
      self.status(env)
      for i in range(25):
        kill(int(read_file(status_params.nifi_ca_pid_file)), SIGTERM)
        time.sleep(1)
        self.status(env)
      kill(int(read_file(status_params.nifi_ca_pid_file)), SIGKILL)
      time.sleep(5)
      self.status(env)
    except ComponentIsNotRunning:
      os.remove(status_params.nifi_ca_pid_file)

if __name__ == "__main__":
  CertificateAuthority().execute()
