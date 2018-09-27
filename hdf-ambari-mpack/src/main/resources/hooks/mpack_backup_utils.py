from __future__ import print_function

import os
import sys

from ambari_server.setupMpacks import get_mpack_properties
from resource_management.core import shell
from resource_management.core import sudo
from resource_management.core.logger import Logger

Logger.initialize_logger()

STACKS_PATH, _, _, _, _ = get_mpack_properties()
RESOURCES_PATH = os.path.dirname(STACKS_PATH)
BACKUP_PATH = os.path.join(RESOURCES_PATH, "backups")

BACKUP_FAIL_MESSAGE = """Failed to backup '{0}'. Possible reasons:
  1. you are using wrong ambari install that does not contain desired path - install correct version of ambari or mpack
  2. you are upgrading or installing mpack on ambari setup with previously purged stacks - restore purged files
"""

RESTORE_FAIL_MESSAGE = """Failed to restore backup '{0}'. Possible reasons:
  1. you are using wrong ambari install that does not contain desired path - install correct version of ambari or mpack
  2. you are upgrading or installing mpack on ambari setup with previously purged stacks - restore purged files
"""

# format (path_to_backup, ("action", action_meta), path_to_restore))
# meta formats:
#   action "copy":
#      bool: indicates if restore path need to be cleaned up
BACKUP_LIST = [
  ("stacks/HDP/3.0/services/SMARTSENSE", ("copy", False), "stacks/HDF/3.2.b/services/SMARTSENSE")
]


def copy_tree(source, destination, remove_destination=True):
  source = source + "/" if source[-1] != "/" else source
  destination = destination + "/" if destination[-1] != "/" else destination
  if remove_destination:
    shell.checked_call(["rm", "-rf", destination], sudo=True)
    sudo.makedirs(destination, 0755)
  elif not sudo.path_exists(destination):
    sudo.makedirs(destination, 0755)
  shell.checked_call(["cp", "-rf", source + ".", destination], sudo=True)


def do_backup():
  if not sudo.path_exists(BACKUP_PATH):
    sudo.makedir(BACKUP_PATH, 0755)
  for backup_item, _, _ in BACKUP_LIST:
    backup_source = os.path.join(RESOURCES_PATH, backup_item)
    backup_destination = os.path.join(BACKUP_PATH, backup_item)
    if sudo.path_exists(backup_source) and sudo.path_isdir(backup_source):
      try:
        copy_tree(backup_source, backup_destination, True)
      except:
        print(BACKUP_FAIL_MESSAGE.format(backup_item), file=sys.stderr)
        raise
    else:
      if not sudo.path_exists(backup_destination):
        print(RESTORE_FAIL_MESSAGE.format(backup_item), file=sys.stderr)
        sys.exit(1)


def do_restore():
  for backup_item, action, destination in BACKUP_LIST:
    backup_item = os.path.join(BACKUP_PATH, backup_item)
    restore_destination = os.path.join(RESOURCES_PATH, destination)
    action, action_meta = action[0], action[1:]
    if action == "copy":
      remove_destination = action_meta[0]
      if sudo.path_exists(backup_item) and sudo.path_isdir(backup_item):
        try:
          copy_tree(backup_item, restore_destination, remove_destination)
        except:
          print(RESTORE_FAIL_MESSAGE.format(backup_item), file=sys.stderr)
          raise
      else:
        print(RESTORE_FAIL_MESSAGE.format(backup_item), file=sys.stderr)
        sys.exit(1)
    else:
      print("Unknown restore action {0} for item {1}".format(action, backup_item), file=sys.stderr)
      sys.exit(1)
