# ElasticClient

A small wrapper for RESTful communication with an ES server. Provides indexing and bulk operations and a limited, generic query builder DSL.


# Configuration
```
config :elastic_client, :server,
  host: "localhost",
  port: 9200
```