# ElasticClient

A small wrapper for RESTful communication with an ES server. Provides indexing and bulk operations and a limited, generic query builder DSL.


# Configuration

config :elasticsearch_client,
  host: "localhost",
  port: 9200
  index_prefix: "a_string_that_will_be_prepended_to_all_new_index_names"