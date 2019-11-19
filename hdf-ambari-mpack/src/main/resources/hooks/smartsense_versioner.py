from __future__ import print_function

import os
import re

from ambari_server.setupMpacks import get_mpack_properties
from resource_management.core import shell
from resource_management.core import sudo
from resource_management.core.logger import Logger

Logger.initialize_logger()

SMARTSENSE_VERSION_TEMPLATE = "{BASE_VERSION}.{AMBARI_VERSION}{SUFFIX}"
VERSION_RE = r"^(([0-9]+)\.([0-9]+)\.([0-9]+))\.([0-9]+)((\.|-).*)?$"

VERSION_REPLACEMENT_PROPERTY = {
  "2.7.100": "${SmartSenseVersion}",
  "2.7.0": "${project.version}",
  "2.7.1": "${project.version}",
  "2.7.2": "${project.version}",
  "2.7.3": "${project.version}",
  "2.7.4": "${project.version}",
  "2.7.5": "${project.version}"
}
FILES_LIST = [
  "metainfo.xml",
  "configuration/hst-agent-conf.xml",
  "package/scripts/hst_script.py",
  "package/scripts/params.py"
]

STACKS_PATH, _, COMMON_SERVICES_PATH, _, _ = get_mpack_properties()

SMARTSENSE_METAINFO_PATH = os.path.join(STACKS_PATH, "HDF", "3.2.b", "services", "SMARTSENSE", "metainfo.xml")
VIEW_JAR_FOLDER_TEMPLATE = os.path.join(COMMON_SERVICES_PATH, "SMARTSENSE", "{_3_DIGIT_VERSION}", "package", "files",
                                        "view")
SMARTSENSE_FOLDER_TEMPLATE = os.path.join(COMMON_SERVICES_PATH, "SMARTSENSE", "{_3_DIGIT_VERSION}")
SMARTSENSE_COMMON_FOLDER_RELATIVE_TEMPLATE = "common-services/SMARTSENSE/{_3_DIGIT_VERSION}"

LOGSEARCH_COMMON_PATH = {
  "2.7.0": "common-services/LOGSEARCH/0.5.0",
  "2.7.1": "common-services/LOGSEARCH/0.5.0",
  "2.7.2": "common-services/LOGSEARCH/0.5.0",
  "2.7.3": "common-services/LOGSEARCH/0.5.0",
  "2.7.4": "common-services/LOGSEARCH/0.5.0",
  "2.7.5": "common-services/LOGSEARCH/0.5.0",
  "2.7.100": "common-services/LOGSEARCH/2.7.100",
}
LOGSEARCH_METAINFO_PATH = os.path.join(STACKS_PATH, "HDF", "3.2.b", "services", "LOGSEARCH", "metainfo.xml")


def get_ambari_version():
  code, out = shell.checked_call(["ambari-server", "--version"], sudo=True)
  possible_version = out.strip()
  match = re.match(VERSION_RE, possible_version)
  if match:
    return possible_version, match.group(1)
  else:
    raise Exception("Failed to get ambari-server version")


def replace_in_file(file_path, what, to):
  file_content = sudo.read_file(file_path, "utf8")
  file_content = file_content.replace(what, to)
  sudo.create_file(file_path, file_content, "utf8")


def select_versions():
  ambari_version, _3_digit_ambari_version = get_ambari_version()
  if _3_digit_ambari_version in ("2.7.3", "2.7.4", "2.7.5"):
    base_version = "1.5.1"
    suffix = ""
  elif _3_digit_ambari_version == "2.7.100":
    base_version = "2.0.0"
    suffix = "-1"
  else:
    base_version = "1.5.0"
    suffix = ""
  desired_version = SMARTSENSE_VERSION_TEMPLATE.format(
    BASE_VERSION=base_version,
    AMBARI_VERSION=ambari_version,
    SUFFIX=suffix
  )

  smartsense_directory = SMARTSENSE_FOLDER_TEMPLATE.format(_3_DIGIT_VERSION=_3_digit_ambari_version)
  for _file in FILES_LIST:
    file_path = os.path.join(smartsense_directory, _file)
    replace_in_file(file_path, VERSION_REPLACEMENT_PROPERTY[_3_digit_ambari_version], desired_version)

  view_jar_folder = VIEW_JAR_FOLDER_TEMPLATE.format(_3_DIGIT_VERSION=_3_digit_ambari_version)
  source_view_jar_file_path = os.path.join(
    view_jar_folder,
    "smartsense-ambari-view-{version}.jar".format(version=_3_digit_ambari_version)
  )
  new_view_jar_file_path = os.path.join(
    view_jar_folder,
    "smartsense-ambari-view-{version}.jar".format(version=desired_version)
  )
  shell.checked_call(["cp", "-f", source_view_jar_file_path, new_view_jar_file_path], sudo=True)

  replace_in_file(
    SMARTSENSE_METAINFO_PATH,
    "${SMARTSENSE_PLACEHOLDER}",
    SMARTSENSE_COMMON_FOLDER_RELATIVE_TEMPLATE.format(_3_DIGIT_VERSION=_3_digit_ambari_version)
  )
  replace_in_file(SMARTSENSE_METAINFO_PATH, "${VERSION_PLACEHOLDER}", desired_version)

  # select proper logsearch version
  replace_in_file(
    LOGSEARCH_METAINFO_PATH,
    "${LOGSEARCH_PLACEHOLDER}",
    LOGSEARCH_COMMON_PATH[_3_digit_ambari_version]
  )
