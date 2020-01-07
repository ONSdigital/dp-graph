dp-graph
================

A library to abstract graph database logic away from services

### Configuration

| Environment variable | Default | Description
| -------------------- | ------- | -----------
| GRAPH_DRIVER_TYPE    |   ""    |  string identifier for the implementation to be used (e.g. 'neo4j' or 'mock')
| GRAPH_ADDR           |   ""    |  address of the database matching the chosen driver type
| GRAPH_POOL_SIZE      |   0     |  desired size of the connection pool
| MAX_RETRIES          |   0     |  maximum number of attempts for transient query failures
| GRAPH_QUERY_TIMEOUT  |   0     |  maximum number of seconds to allow a query before timing out

All config other than `GRAPH_DRIVER_TYPE` will be subject to that implementation to make use of
and set reasonable defaults for use in that context. It's feasible that some implementations
might not have configurable timeouts for example, so whether this can be set should be
documented in each driver.

### Design

See [DESIGN](DESIGN-NOTES.md) for details.

### Health package

The Graph checker function is currently implemented only for Neo4J. It connects to Neo4J and performs a 'ping query', just to validate that we can communicate with it. The health check will succeed only if the query succeeds.

Read the [Health Check Specification](https://github.com/ONSdigital/dp/blob/master/standards/HEALTH_CHECK_SPECIFICATION.md) for details.

Instantiate a Neo4J client
```
    cli := neo4jdriver.New(<dbAddr>, <size>, <timeout>)
```

Call Neo4J health checker with `cli.Checker(context.Background())` and this will return a check object:

```
{
    "name": "string",
    "status": "string",
    "message": "string",
    "status_code": "int",
    "last_checked": "ISO8601 - UTC date time",
    "last_success": "ISO8601 - UTC date time",
    "last_failure": "ISO8601 - UTC date time"
}
```

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2019, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
