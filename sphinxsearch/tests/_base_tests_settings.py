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
    sql_query = SELECT * FROM "base_nazyaproduct"
    enable_star = 1
    path = /var/www/nazya/nazya/var/sphinx/index_data/data_anyshop_products
    docinfo = extern
    morphology = stem_enru
    min_word_len = 2
    charset_type = utf-8
    charset_table = 0..9, A..Z->a..z, _, a..z, U+410..U+42F->U+430..U+44F, U+430..U+44F
    min_infix_len = 2
    query_info = SELECT * FROM "base_nazyaproduct" WHERE id=$id
    sql_attr_timestamp = modified_at
    sql_attr_float = post_fee
    sql_attr_string = thumbs
    sql_attr_bool = in_stock
    sql_attr_uint = type
    sql_attr_multi = uint property_values_ids from query;
SELECT "base_nazyaproduct_property_values"."nazyaproduct_id"
}}

index {0}
{{
    source = {0}
}}
"""
