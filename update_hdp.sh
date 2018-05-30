#!/usr/bin/env bash
repo_proto=${1:-https}
repo_branch=${2:-AMBARI-2.7.0.0}
username_password=$3
username_password=$( [ "$username_password" ] && echo "${username_password}@")
repo_url=$(([ "$repo_proto" == "ssh" ] && echo "git@github.com:hortonworks/hdp_ambari_definitions.git") || ([ "$repo_proto" == "https" ] && echo "https://${username_password}github.com/hortonworks/hdp_ambari_definitions.git") || exit -1)
[ $? -ne 0 ] && (echo "Unknown protocol $repo_proto, must be ssh or https"; exit -1)
rm -rf hdp_ambari_definitions
rm -rf .git/modules/hdp_ambari_definitions
git config --file=.gitmodules submodule.hdp_ambari_definitions.url $repo_url
git config --file=.gitmodules submodule.hdp_ambari_definitions.branch $repo_branch
git submodule init
git submodule sync
git submodule update --remote
