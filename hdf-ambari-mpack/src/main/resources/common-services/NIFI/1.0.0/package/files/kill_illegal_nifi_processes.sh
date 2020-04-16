#!/bin/bash -e

function kill_illegal_nifi_processes () {
    GREP_COMMAND=$1

    for pid in $(ps -ef | eval ${GREP_COMMAND} | awk '{print $2}'); do
      kill -SIGTERM $pid
      i=0
      until [ $i -ge 30 ]
        do
          if ! ps -fp $pid > /dev/null; then break; fi
          i=$[$i+1]
          sleep 1
        done

      if [ $i -ge 30 ] && ps -fp $pid | eval ${GREP_COMMAND} > /dev/null; then
        kill -9 $pid || true
      fi
    done
}

kill_illegal_nifi_processes "$1"