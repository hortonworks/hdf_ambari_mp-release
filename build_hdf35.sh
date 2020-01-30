pushd hdf-ambari-mpack
mvn versions:set -DnewVersion=3.5.0.0-1
mvn clean package -DminAmbariVersion=2.7.0 -DmaxAmbariVersion=2.7.5 -Phorton
popd
