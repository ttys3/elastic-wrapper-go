# elastic-wrapper

A tiny wrapper for elasticsearch, supporting common operations with friendly interfaces.

## documentation

https://www.elastic.co/guide/en/elasticsearch/reference/8.4/api-conventions.html

### JSON encoding

> Elasticsearch only supports UTF-8-encoded JSON. 
> Elasticsearch ignores any other encoding headings sent with a request. 
> Responses are also UTF-8 encoded.


### tracing

> Elasticsearch also supports a traceparent HTTP header using the official W3C trace context spec. 
> You can use the `traceparent` header to trace requests across Elastic products and other services. 
> Because it’s only used for traces, you can safely generate a unique `traceparent` header for each request.

> If provided, Elasticsearch surfaces the header’s trace-id value as `trace.id` in the:

[JSON Elasticsearch server logs](https://www.elastic.co/guide/en/elasticsearch/reference/8.4/logging.html)
[Slow logs](https://www.elastic.co/guide/en/elasticsearch/reference/8.4/index-modules-slowlog.html#_identifying_search_slow_log_origin)
[Deprecation logs](https://www.elastic.co/guide/en/elasticsearch/reference/8.4/logging.html#deprecation-logging)

### GET and POST requests
> A number of Elasticsearch GET APIs—​most notably the search API—​support a request body. 
> While the GET action makes sense in the context of retrieving information, GET requests with a body are not supported by all HTTP libraries. 
> All Elasticsearch GET APIs that require a body can also be submitted as POST requests. 
> Alternatively, you can pass the request body as the source query string parameter when using GET.

### REST API version compatibility

elasticsearch server REST API version compatibility

https://www.elastic.co/guide/en/elasticsearch/reference/8.4/api-conventions.html#api-compatibility

### Number Values
All REST APIs support providing numbered parameters as string on top of supporting the native JSON number types.

### Distance Units

Wherever distances need to be specified, such as the distance parameter 
in the [Geo-distance](https://www.elastic.co/guide/en/elasticsearch/reference/8.4/query-dsl-geo-distance-query.html), 
the default unit is meters if none is specified. Distances can be specified in other units, such as "1km" or "2mi" (2 miles).

The full list of units is listed below:

https://www.elastic.co/guide/en/elasticsearch/reference/8.4/api-conventions.html#distance-units

### client SDK compatibility

https://www.elastic.co/guide/en/elasticsearch/client/go-api/current/installation.html#_elasticsearch_version_compatibility

Elasticsearch Version Compatibility

Language clients are forward compatible; meaning that clients support communicating with greater or equal minor versions of Elasticsearch. 

Elasticsearch language clients are only backwards compatible with default distributions and without guarantees made.

### Typed API

https://www.elastic.co/guide/en/elasticsearch/client/go-api/current/typedapi.html

* go sdk v8 begin support Typed API

> The goal for this API is to provide a strongly typed, fluent-like Golang API for Elasticsearch.

> This was designed with structures and the Golang runtime in mind, following as closely as possible the API and its objects.

> The first version focuses on the requests and does not yet include [NDJSON](http://ndjson.org/) endpoints such as bulk or msearch. 
> These will be added later on along with typed responses and error handling.``

NDJSON and JSON streaming （https://en.wikipedia.org/wiki/JSON_streaming）

[Newline delimited JSON is awesome](https://medium.com/@kandros/newline-delimited-json-is-awesome-8f6259ed4b4b)

* All the available endpoints are generated in separate packages and assembled in the client. The core namespace is duplicated at the root of the client for convenient access.
  
Each endpoint follows a factory pattern which returns a pointer to a new instance each time. This leads to a builder pattern allowing to directly chain the options before running your query.

* `Do` need `context`, For body-empty endpoints such as `core.Exists`, an additional method `IsSuccess` is available.

* Responsese: While not part of the initial release responses will be added at a later date.