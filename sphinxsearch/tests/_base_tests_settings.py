# -*- coding: utf-8 -*-

TEST_ENGINE_SETTINGS = """searchd
{
    log = /tmp/sphinxtest/logs/searchd.log
    read_timeout = 5
    client_timeout = 300
    max_children = 0
    pid_file = /tmp/sphinxtest/searchd.pid
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


TEST_ENGINE_SCHEMA_SETTINGS = """searchd
{{
    log = /tmp/sphinxtest/logs/searchd.log
    read_timeout = 5
    client_timeout = 300
    max_children = 0
    pid_file = /tmp/sphinxtest/searchd.pid
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
    type = pgsql
    sql_host = localhost
    sql_port = 5432
    sql_db = nazya_db
    sql_user = nazya
    sql_pass = pass
    sql_query = SELECT * FROM "base_nazyaproduct"
    sql_attr_timestamp = modified_at
    sql_attr_float = post_fee
    sql_attr_string = thumbs
    sql_attr_bool = in_stock
    sql_attr_uint = type
    sql_attr_multi = uint property_values_ids from query; SELECT "base_nazyaproduct_property_values"."nazyaproduct_id"
}}

index {0}
{{
    path = /tmp/sphinxtest/index_data
    docinfo = extern
    morphology = stem_enru
    min_word_len = 2
    charset_table = 0..9, A..Z->a..z, _, a..z, U+410..U+42F->U+430..U+44F, U+430..U+44F
    min_infix_len = 2
    source = {0}
}}
"""
