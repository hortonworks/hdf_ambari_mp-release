#### HDF Ambari Management Pack

#### Install maven 3.0.5
```
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
su -c "tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/" 
export M2_HOME=/opt/apache-maven-3.0.5
export M2=$M2_HOME/bin
PATH=$M2:$PATH
echo "export M2_HOME=/opt/apache-maven-3.0.5" >> ~/.bashrc
echo "export M2=$M2_HOME/bin" >> ~/.bashrc
echo "PATH=$M2:$PATH" >> ~/.bashrc
```

#### Clone git
```
yum install -y git
git clone https://<git_userid>@github.com/hortonworks/hdf_ambari_mp.git; cd hdf_ambari_mp; ./update_hdp.sh
```
You will be promted for password twice - first time for main HDF repo and another one for HDP repository.

If you want to use ssh protocol(password-less way of clonning) use following command:
```
git clone git@github.com:hortonworks/hdf_ambari_mp.git; cd hdf_ambari_mp; ./update_hdp.sh ssh
```
You can specify branch of HDP 3.0 stack by passing branch name to `./update_hdp.sh https $branch_name` script as second argument,
also note, that in this case protocol(first agrument, can be ssh or https is mandatory)
for examplem to fetch :
```
./update_hdp.sh https AMBARI-2.7.0.2
```
#### Update submodules
Make sure your submodules is updated. **Always** call `./update_hdp.sh` script before building mpack to fetch latest
submodules definitions.

**NOTE:** after calling `./update_hdp.sh` you can notice `modified:   hdp_ambari_definitions (new commits)` in `git status`,
it is safe and recommended to add this changes to your commits to HDF mpack. This happens due to submodule storing specific
commit reference, so when you updating submodule, your commit reference are updated to latest commit from remote repo of submodule
and you can observe this changes.

You can ommit this changes from your commit, but make sure to call `./update_hdp.sh` before building!
#### Build Instructions:
```
cd hdf_ambari_mp/hdf-ambari-mpack
mvn versions:set -DnewVersion=${HDF_MPACK_VERSION}
mvn clean package -DminAmbariVersion=${MIN_AMBARI_VERSION} -DmaxAmbariVersion=${MAX_AMBARI_VERSION} -Dnifiversion={NIFI_STACK_BUILD_VERSION}
# HDF management pack will be created at target/hdf-ambari-mpack-${HDF_MPACK_VERSION}.tar.gz
```

##### Example:
```
cd hdf_ambari_mp/hdf-ambari-mpack
mvn versions:set -DnewVersion=0.1.0.0-1
mvn clean package -DminAmbariVersion=2.4.0.0 -DmaxAmbariVersion=2.5.0.0  -Dnifiversion=1.2.0.3.0.0.0-137

#this will build the below tarball
ls -la target/hdf-ambari-mpack-0.1.0.0-1.tar.gz
```

#### Installation Instructions:
- Install ambari-server
```
yum clean all
yum install ambari-server -y
ambari-server setup -s
```
- Install HDF mpack
```
ambari-server install-mpack --mpack=/path/to/hdf-ambari-mpack-${HDF_MPACK_VERSION}.tar.gz --purge --verbose
```
  - Example:
```
ambari-server install-mpack --mpack=target/hdf-ambari-mpack-0.1.0.0-1.tar.gz --purge --verbose
```
- Start ambari-server
```
ambari-server start
```
