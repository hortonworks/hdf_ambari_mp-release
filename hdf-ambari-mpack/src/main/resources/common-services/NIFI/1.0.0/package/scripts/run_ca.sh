#!/bin/bash

# Launches NiFi CA server
# $1 -> JAVA_HOME
# $2 -> tls-toolkit.sh path
# $3 -> config json
# $4 -> stdout log
# $5 -> stderr log
# $6 -> pid file

JAVA_HOME="$1" nohup "$2" server -F -f "$3" > "$4" 2> "$5" < /dev/null &
echo $! > "$6"

#Want to wait until Jetty starts
#See http://superuser.com/questions/270529/monitoring-a-file-until-a-string-is-found#answer-900134
( tail -f -n +1 "$4" & ) | timeout 180 grep -q "Server Started"
