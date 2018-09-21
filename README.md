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
git clone https://<git_userid>@github.com/hortonworks/hdf_ambari_mp.git
```
#### Build Instructions:
```
cd hdf_ambari_mp/hdf-ambari-mpack
mvn versions:set -DnewVersion=${HDF_MPACK_VERSION}
mvn clean package -DminAmbariVersion=${MIN_AMBARI_VERSION} -DmaxAmbariVersion=${MAX_AMBARI_VERSION}
# HDF management pack will be created at target/hdf-ambari-mpack-${HDF_MPACK_VERSION}.tar.gz
```

##### Example:
```
cd hdf_ambari_mp/hdf-ambari-mpack
mvn versions:set -DnewVersion=0.1.0.0-1
mvn clean package -DminAmbariVersion=2.4.0.0 -DmaxAmbariVersion=2.5.0.0

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
