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

from time import sleep

from getgauge.python import step
from kylin_utils import util


class LoginTest:

    @step("Initialize <browser_type> browser and connect to <file_name>")
    def setup_browser(self, browser_type, file_name):
        global browser
        browser = util.setup_browser(browser_type=browser_type)

        browser.get(util.kylin_url(file_name))
        sleep(3)

        browser.refresh()
        browser.set_window_size(1400, 800)

    @step("Authentication with user <user> and password <password>.")
    def assert_authentication_failed(self, user, password):
        browser.find_element_by_id("username").clear()
        browser.find_element_by_id("username").send_keys(user)
        browser.find_element_by_id("password").clear()
        browser.find_element_by_id("password").send_keys(password)

        browser.find_element_by_class_name("bigger-110").click()

    @step("Authentication with built-in user <table>")
    def assert_authentication_success(self, table):
        for i in range(1, 2):
            user = table.get_row(i)
            browser.find_element_by_id("username").clear()
            browser.find_element_by_id("username").send_keys(user[0])
            browser.find_element_by_id("password").clear()
            browser.find_element_by_id("password").send_keys(user[1])
            browser.find_element_by_class_name("bigger-110").click()
