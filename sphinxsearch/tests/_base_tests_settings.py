# -*- coding: utf-8 -*-

TEST_ENGINE_SETTINGS = """server
{
    log = /tmp/logs/searchd.log
    read_timeout = 5
    client_timeout = 300
    max_children = 0
    pid_file = /tmp/searchd.pid
    max_matches = 10000
    seamless_rotate = True
    preopen_indexes = True
    workers = prefork
    max_filter_values = 8192
    listen = localhost:4321
}

indexer
{
    mem_limit = 32M
}
"""


TEST_ENGINE_SCHEMA_SETTINGS = """server
{{
    log = /tmp/logs/searchd.log
    read_timeout = 5
    client_timeout = 300
    max_children = 0
    pid_file = /tmp/searchd.pid
    max_matches = 10000
    seamless_rotate = True
    preopen_indexes = True
    workers = prefork
    max_filter_values = 8192
    listen = localhost:4321
}}

indexer
{{
    mem_limit = 32M
}}

source {0}
{{
    sql_attr_uint = nazyacategory_id
    sql_attr_string = orig_name
    sql_attr_float = orig_price
    sql_query = SELECT * FROM "base_nazyaproduct"
    sql_attr_bool = in_stock
    sql_attr_multi = uint property_values_ids from query;
SELECT "base_nazyaproduct_property_values"."nazyaproduct_id"
    sql_attr_timestamp = modified_at
}}

index {0}
{{
    source = {0}
}}
"""
