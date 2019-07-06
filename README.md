# ElasticClient

A small wrapper for RESTful communication with an ES server. Provides indexing and bulk operations and a limited, generic query builder DSL.

Requires Elastic server v 7.0+


# Configuration
```
config :elastic_client, :server,
  host: "localhost",
  port: 9200
```