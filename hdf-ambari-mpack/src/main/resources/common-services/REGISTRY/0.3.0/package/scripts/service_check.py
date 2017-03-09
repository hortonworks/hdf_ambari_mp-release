#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.show_logs import show_logs
import urllib2

class ServiceCheck(Script):
  def service_check(self, env):
    import params
    env.set_params(params)
    Logger.info("Registry check passed")
    registry_api = format("http://{params.hostname}:{params.registry_port}/")
    Logger.info(registry_api)
    try:
        Logger.info(format("Making http requests to {registry_api}"))
        response = urllib2.urlopen(registry_api)
        api_response = response.read()
        response_code = response.getcode()
        Logger.info(format("registry response http status {response_code}"))
        if response.getcode() != 200:
            Logger.error(format("Failed to fetch response for {registry_api}"))
            raise
    except:
        Logger.error("Failed to make API request to Registry server")
        show_logs(params.registry_log_dir, params.registry_user)
        raise

if __name__ == "__main__":
    ServiceCheck().execute()
