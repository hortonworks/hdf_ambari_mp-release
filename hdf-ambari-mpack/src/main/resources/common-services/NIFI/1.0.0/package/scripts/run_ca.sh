#!/bin/bash

# Launches NiFi CA server
# $1 -> JAVA_HOME
# $2 -> tls-toolkit.sh path
# $3 -> config json
# $4 -> stdout log
# $5 -> stderr log
# $6 -> pid file

die() {
  echo "$1"
  exit 1
}

read -r -d '' WAIT_FOR_LOG << 'EOF'
  STDOUT_LOG="$0"
  until [ -f "$STDOUT_LOG" ]; do
    echo "Waiting for $STDOUT_LOG to exist"
    sleep 1;
  done
EOF

read -r -d '' WAIT_FOR_CHILD << 'EOF'
  SHELL_PID="$0"
  PID_FILE="$1"
  CHILD_PROCESS="$(pgrep -P "$SHELL_PID" java)"
  while [ -z "$CHILD_PROCESS" ]; do
    echo "Waiting for child java process to exist"
    sleep 1;
    CHILD_PROCESS="$(pgrep -P "$SHELL_PID" java)"
  done
  echo "$CHILD_PROCESS" > "$PID_FILE"
EOF

JAVA_HOME="$1" nohup "$2" server -F -f "$3" > "$4" 2> "$5" < /dev/null &
SHELL_PID="$!"

timeout 30 bash -c "$WAIT_FOR_LOG" "$4" || die "Timed out while waiting for $4"

timeout 30 bash -c "$WAIT_FOR_CHILD" "$SHELL_PID" "$6" || die "Timed out while waiting for child java process to exist"

#Want to wait until Jetty starts
#See http://superuser.com/questions/270529/monitoring-a-file-until-a-string-is-found#answer-900134
( tail -f -n +1 "$4" & ) | timeout 180 grep -q "Server Started" || die "Timed out while waiting for CA server to start"
