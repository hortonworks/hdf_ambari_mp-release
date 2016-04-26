#### HDF Ambari Management Pack

#### Build Instructions:
```
cd hdf-ambari-mpack
mvn versions:set -DnewVersion=${HDF_MPACK_VERSION}
mvn clean package -DminAmbariVersion=${MIN_AMBARI_VERSION} -DmaxAmbariVersion=${MAX_AMBARI_VERSION}
# HDF management pack will be created at target/hdf-ambari-mpack-${HDF_MPACK_VERSION}.tar.gz
```

#### Example:
```
cd hdf-ambari-mpack
mvn versions:set -DnewVersion=0.1.0.0-1
mvn clean package -DminAmbariVersion=2.4.0.0 -DmaxAmbariVersion=
ls target/hdf-ambari-mpack-0.1.0.0-1.tar.gz
target/hdf-ambari-mpack-0.1.0.0-1.tar.gz
```
