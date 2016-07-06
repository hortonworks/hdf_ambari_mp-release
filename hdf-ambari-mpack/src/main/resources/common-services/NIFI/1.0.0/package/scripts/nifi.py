import sys, os, pwd, grp, signal, time, glob, socket
from resource_management import *
from subprocess import call
from setup_ranger_nifi import setup_ranger_nifi

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
  def install(self, env):

    import params
    import status_params

    self.install_packages(env)

    #Create user and group if they don't exist
    self.create_linux_user(params.nifi_user, params.nifi_group)

    #create the log, pid, conf dirs if not already present
    #Directory([status_params.nifi_pid_dir, params.nifi_node_log_dir, params.conf_dir, params.work_dir],
    #        owner=params.nifi_user,
    #        group=params.nifi_group,
    #        create_parents=True
    #)

    Execute('touch ' +  params.nifi_node_log_file, user=params.nifi_user)

    Execute('chown '+params.nifi_user+':'+params.nifi_group+' '+params.nifi_node_dir)
    Execute('chown -R '+params.nifi_user+':'+params.nifi_group+' '+params.nifi_node_dir+'/*')

    #update the configs specified by user
    self.configure(env, True)



  def create_linux_user(self, user, group):
    try: pwd.getpwnam(user)
    except KeyError: Execute('adduser ' + user)
    try: grp.getgrnam(group)
    except KeyError: Execute('groupadd ' + group)



  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)

    #write out nifi.properties
    params.nifi_node_properties_content=params.nifi_node_properties_content.replace("{{nifi_node_host}}",socket.getfqdn())
    #properties_content=InlineTemplate(params.nifi_node_properties_content)
    File(format("{params.conf_dir}/nifi.properties"), content=InlineTemplate(params.nifi_node_properties_content), owner=params.nifi_user, group=params.nifi_group) # , mode=0777)

    # create the nifi flow config dir if it doesn't exist, and change ownership to NiFi user
    if not os.path.exists(format("{params.nifi_flow_config_dir}")):
        os.makedirs(format("{params.nifi_flow_config_dir}"))
    Execute('chown ' + params.nifi_user + ':' + params.nifi_group + ' ' + format("{params.nifi_flow_config_dir}"))

    #write out flow.tar only if AMS installed and during first setup
    #it is used to automate setup of Ambari metrics reporting task in Nifi
    if isInstall and params.metrics_collector_host:
      Execute('echo "First time setup so generating flow.tar" >> ' + params.nifi_node_log_file)
      flow_content=InlineTemplate(params.nifi_flow_content)
      File(format("{params.nifi_flow_config_dir}/flow.xml"), content=flow_content, owner=params.nifi_user, group=params.nifi_group)
      Execute(format("cd {params.nifi_flow_config_dir}; mv flow.tar flow_$(date +%d-%m-%Y).tar ;"), user=params.nifi_user, ignore_failures=True)
      Execute(format("cd {params.nifi_flow_config_dir}; tar -cvf flow.tar flow.xml;"), user=params.nifi_user)

    #write out boostrap.conf
    bootstrap_content=InlineTemplate(params.nifi_boostrap_content)
    File(format("{params.conf_dir}/bootstrap.conf"), content=bootstrap_content, owner=params.nifi_user, group=params.nifi_group)

    #write out logback.xml - this does not seem to be required now: the log dir can be controlled by nifi-env.sh
    #logback_content=InlineTemplate(params.nifi_node_logback_content)
    #File(format("{params.conf_dir}/logback.xml"), content=logback_content, owner=params.nifi_user, group=params.nifi_group)

    #write out state-management.xml
    statemgmt_content=InlineTemplate(params.nifi_state_management_content)
    File(format("{params.conf_dir}/state-management.xml"), content=statemgmt_content, owner=params.nifi_user, group=params.nifi_group)

    #write out authorizers file
    authorizers_content=InlineTemplate(params.nifi_authorizers_content)
    File(format("{params.conf_dir}/authorizers.xml"), content=authorizers_content, owner=params.nifi_user, group=params.nifi_group)

    #write out login-identity-providers.xml
    login_identity_providers_content=InlineTemplate(params.nifi_login_identity_providers_content)
    File(format("{params.conf_dir}/login-identity-providers.xml"), content=login_identity_providers_content, owner=params.nifi_user, group=params.nifi_group)

    #write out nifi-env in bin
    env_content=InlineTemplate(params.nifi_env_content)
    File(format("{params.bin_dir}/nifi-env.sh"), content=env_content, owner=params.nifi_user, group=params.nifi_group) 
    #File(format("{params.bin_dir}/nifi-env.sh"), content=env_content) 


  def stop(self, env):
    import params
    import status_params

    Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh stop >> ' + params.nifi_node_log_file, user=params.nifi_user)
    #Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh stop >> ' + params.nifi_node_log_file)
    if os.path.isfile(status_params.nifi_node_pid_file):
      Execute ('rm ' + status_params.nifi_node_pid_file)



  def start(self, env):
    import params
    import status_params
    self.configure(env)
    setup_ranger_nifi(upgrade_type=None)

    Execute('echo nifi nodes: ' + params.nifi_node_hosts)
    Execute('echo pid file ' + status_params.nifi_node_pid_file)
    Execute('echo JAVA_HOME=' + params.jdk64_home)

    Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh start >> ' + params.nifi_node_log_file, user=params.nifi_user)
    #Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh start >> ' + params.nifi_node_log_file)
    Execute('cat '+status_params.nifi_pid_dir+'/nifi.pid'+" | grep pid | sed 's/pid=\(\.*\)/\\1/' > " + status_params.nifi_node_pid_file)
    Execute('chown '+params.nifi_user+':'+params.nifi_group+' ' + status_params.nifi_node_pid_file)


  def status(self, env):
    import status_params
    check_process_status(status_params.nifi_node_pid_file)




if __name__ == "__main__":
  Master().execute()
