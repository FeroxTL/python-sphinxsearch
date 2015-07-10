# -*- coding: utf-8 -*-

TEST_ENGINE_SETTINGS = """indexer
{
    mem_limit = 32M
}

server
{
    log = /tmp/logs/searchd.log
    max_children = 0
    workers = prefork
    max_matches = 10000
    pid_file = /tmp/searchd.pid
    max_filter_values = 8192
    read_timeout = 5
    preopen_indexes = True
    seamless_rotate = True
    listen = localhost:4321
    client_timeout = 300
}
"""


TEST_ENGINE_PART_1 = """source {0}
{{
    sql_attr_timestamp = created_at
    sql_attr_string = images
    sql_attr_bool = in_stock
    sql_attr_multi = uint property_values_ids from query;
SELECT "base_nazyaproduct_property_values"."nazyaproduct_id"
    sql_attr_float = current_price
    sql_attr_uint = nazyacategory_id
}}"""

TEST_ENGINE_PART_2 = """index {0}
{{
    source = {0}
}}"""

TEST_ENGINE_PART_3 = """indexer
{{
    mem_limit = 32M
}}"""

TEST_ENGINE_PART_4 = """server
{{
    log = /tmp/logs/searchd.log
    max_children = 0
    workers = prefork
    max_matches = 10000
    pid_file = /tmp/searchd.pid
    max_filter_values = 8192
    read_timeout = 5
    preopen_indexes = True
    seamless_rotate = True
    listen = localhost:4321
    client_timeout = 300
}}"""


TEST_ENGINE_SCHEMA_SETTINGS_LIST = [
    TEST_ENGINE_PART_1,
    TEST_ENGINE_PART_2,
    TEST_ENGINE_PART_3,
    TEST_ENGINE_PART_4,
]
