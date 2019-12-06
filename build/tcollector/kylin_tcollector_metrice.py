#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import sys
import json

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection


class Kylin(object):
    def __init__(self, service, daemon, host, port, uri="/kylin/api/jmetrics"):
        self.service = service
        self.daemon = daemon
        self.port = port
        self.host = host
        self.uri = uri
        self.server = HTTPConnection(self.host, self.port)
        self.server.auto_open = True

    def request(self):
        try:
            self.server.request('GET', self.uri)
            resp = self.server.getresponse().read()
        except:
            resp = '{}'
        finally:
            self.server.close()
        return json.loads(resp)

    def poll(self):
        """
        Get metrics from the http server's /kylin/api/metrics page, and transform them into normalized tupes
        """
        json_obj = self.request()
        kept = []
        for i in json_obj:
            if i == 'version': continue
            for j in json_obj[i]:
                for k in json_obj[i][j]:
                    context = i + "." + k
                    value = str(json_obj[i][j][k])
                    key = j.split('metrics:')[-1].replace(',', ' ')
                    if key.find('CUBE[name->') > -1:
                        key = key.replace('CUBE[name->', '').replace(']', '')
                    kept.append((context, key, value))
        return kept

    def emit_metric(self, context, current_time, metric_name, value):
        print("{}.{}.{} {} {} {}".format(self.service, self.daemon, context, current_time, value, metric_name))
        sys.stdout.flush()

    def emit(self):
        """
        Emit metrics from a Kylin server.
        """
        current_time = int(time.time())
        metrics = self.poll()

        for context, metric_name, value in metrics:
            self.emit_metric(context, current_time, metric_name, value)


def main(args):
    kylin_service = Kylin("kylin", "server", "localhost", 7070)

    while True:
        kylin_service.emit()
        time.sleep(15)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
