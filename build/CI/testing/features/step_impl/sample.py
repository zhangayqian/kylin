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

from getgauge.python import step, before_spec
from kylin_utils import util
from kylin_utils import equals


@before_spec()
def before_spec_hook():
    global client
    client = util.setup_instance("kylin_instance.yml")


@step("Query sql <select count(*) from kylin_sales> in <project> and compare result with pushdown result")
def query_sql_and_compare_result_with_pushdown_result(sql, project):
    equals.compare_sql_result(sql=sql, project=project, kylin_client=client)
