import sys, os, pwd, grp, signal, time, glob, socket
from resource_management import *
from subprocess import call

reload(sys)
sys.setdefaultencoding('utf8')

class Master(Script):
  def install(self, env):

    import params
    import status_params
      
    #Execute('echo master config dump: ' + str(', '.join(params.master_configs)))
    
    #official HDF 1.2 package (nifi 0.6.0)
    snapshot_package='http://public-repo-1.hortonworks.com/HDF/centos6/1.x/updates/1.2.0.0/HDF-1.2.0.0-91.zip'
    

    #e.g. /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/NIFI/package
    service_packagedir = os.path.realpath(__file__).split('/scripts')[0] 
            
    #Execute('find '+service_packagedir+' -iname "*.sh" | xargs chmod +x')

    #Create user and group if they don't exist
    self.create_linux_user(params.nifi_user, params.nifi_group)
    if params.nifi_user != 'root':
      Execute('cp /etc/sudoers /etc/sudoers.bak')        
      Execute('echo "'+params.nifi_user+'    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers')
      Execute('echo Creating ' +  params.nifi_master_log_dir +  ' ' +  status_params.nifi_pid_dir)    

            
    #create the log dir if it not already present
    Directory([status_params.nifi_pid_dir, params.nifi_master_log_dir],
            owner=params.nifi_user,
            group=params.nifi_group,
            create_parents=True
    )   
         
    Execute('touch ' +  params.nifi_master_log_file, user=params.nifi_user)    
    Execute('rm -rf ' + params.nifi_master_dir, ignore_failures=True)
    
    Directory([params.nifi_master_dir],
            owner=params.nifi_user,
            group=params.nifi_group,
            create_parents=True
    )      
    
        
    #Fetch and unzip snapshot build, if no cached nifi tar package exists on Ambari server node
    if not os.path.exists(params.temp_file):
      Execute('wget '+snapshot_package+' -O '+params.temp_file+' -a '  + params.nifi_master_log_file, user=params.nifi_user)
    Execute('unzip '+params.temp_file+' -d ' + params.nifi_master_dir + ' >> ' + params.nifi_master_log_file, user=params.nifi_user)
    Execute('mv '+params.nifi_master_dir+'/*/*/* ' + params.nifi_master_dir, user=params.nifi_user)
                
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
    
    self.set_conf_bin(env)
    params.nifi_node_host = socket.gethostname()
    #write out nifi.properties
    properties_content=InlineTemplate(params.nifi_master_properties_content)
    File(format("{params.conf_dir}/nifi.properties"), content=properties_content, owner=params.nifi_user, group=params.nifi_group) # , mode=0777)    

    #write out flow.tar only during install to setup Ambari metrics reporting task in Nifi
    if isInstall:
      Execute('echo "First time setup so generating flow.tar" >> ' + params.nifi_master_log_file)    
      flow_content=InlineTemplate(params.nifi_flow_content)
      File(format("{params.conf_dir}/flow.xml"), content=flow_content, owner=params.nifi_user, group=params.nifi_group)
      Execute(format("cd {params.conf_dir}; mv flow.tar flow_$(date +%d-%m-%Y).tar ;"), user=params.nifi_user, ignore_failures=True)
      Execute(format("cd {params.conf_dir}; tar -cvf flow.tar flow.xml;"), user=params.nifi_user)

    #write out boostrap.conf
    bootstrap_content=InlineTemplate(params.nifi_boostrap_content)
    File(format("{params.conf_dir}/bootstrap.conf"), content=bootstrap_content, owner=params.nifi_user, group=params.nifi_group) 

    #write out logback.xml
    logback_content=InlineTemplate(params.nifi_master_logback_content)
    File(format("{params.conf_dir}/logback.xml"), content=logback_content, owner=params.nifi_user, group=params.nifi_group) 
    
    
    
  def stop(self, env):
    import params
    import status_params    
    self.set_conf_bin(env)    
    Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh stop >> ' + params.nifi_master_log_file, user=params.nifi_user)
    Execute ('rm ' + status_params.nifi_master_pid_file)
 
      
  def start(self, env):
    import params
    import status_params
    self.configure(env) 
    self.set_conf_bin(env)    
    Execute('echo nifi nodes: ' + params.nifi_node_hosts)
    Execute('echo pid file ' + status_params.nifi_master_pid_file)
    Execute('echo JAVA_HOME=' + params.jdk64_home)

    Execute ('export JAVA_HOME='+params.jdk64_home+';'+params.bin_dir+'/nifi.sh start >> ' + params.nifi_master_log_file, user=params.nifi_user)

    Execute('cat '+params.bin_dir+'/nifi.pid'+" | grep pid | sed 's/pid=\(\.*\)/\\1/' > " + status_params.nifi_master_pid_file)
    Execute('chown '+params.nifi_user+':'+params.nifi_group+' ' + status_params.nifi_master_pid_file)
    
  def status(self, env):
    import status_params       
    check_process_status(status_params.nifi_master_pid_file)


  def set_conf_bin(self, env):
    import params
  
    params.conf_dir = os.path.join(*[params.nifi_master_dir,'conf'])
    params.bin_dir = os.path.join(*[params.nifi_master_dir,'bin'])

      
if __name__ == "__main__":
  Master().execute()
