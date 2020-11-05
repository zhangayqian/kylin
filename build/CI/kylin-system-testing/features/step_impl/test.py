import os
import json
from kylin_utils import util
from kylin_utils import equals


def query_sql_file_and_compare():

    sql_directory = "../../query/sql/sql_test/"
    sql_result_directory = "../../query/sql_result/sql_test/"
    sql_directory_list = os.listdir(sql_directory)
    for sql_file_name in sql_directory_list:
        with open(sql_directory + sql_file_name, 'r', encoding='utf8') as sql_file:
            sql = sql_file.read()

        client = util.setup_instance('kylin_instance.yml')
        expected_result_file_name = sql_result_directory + sql_file_name.split(".")[0]
        expected_result = None
        if os.path.exists(expected_result_file_name):
            with open(sql_result_directory + sql_file_name.split(".")[0] + '.json', 'r', encoding='utf8') as expected_result_file:
                expected_result = json.loads(expected_result_file.read())
        equals.compare_sql_result(sql=sql, project="generic_test_project", kylin_client=client, expected_result=expected_result)
        print("succeed")


if __name__ == '__main__':
    query_sql_file_and_compare()
    # print(__name__)