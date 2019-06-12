# Design Notes

The code in the dp-graph repository provides a generalised interface to 
diverse graph databases - for a particular set of business application 
use-cases. This document explains something of the code organisation, its 
approach to the separation of concerns, and composition.

> Nb. I reverse engineered this document from the code (Pete) - so 
don't assume it is completely correct.

One such business application use-case is formed from the requirements that 
the **CodeList** API service has for a graph database. We'll use this as
an example.

## API

The library API is defined by the graph and graph.driver packages.

The **graph** package provides the **DB** type (a struct) - which is the
object clients interact with. Clients interested in the Code List use-case
can construct a DB specialised to this case with `graph.NewCodeListStore()`

Other use-cases are catered for; like `NewHierarchyStore()`.

The API contract offered by `NewCodeListStore()` is defined by the 
`graph.driver.CodeList` interface. For example `GetCodeList()`.

## Diverse Database Implementations

The dp-graph repo contains both Neptune and Neo4J directories, providing
their own `graph.driver` implementation as well as their `CodeList` (and family)
implementations. Typically these exploit an external low-level database driver.
For example `gedge/gremlin-neptune`. They then typically provide a convenience 
library containing methods like `getVertices()` that uses the lower level 
driver, and makes it simpler to satisfy the methods for `CodeList` etc.

## Composition and Configuration

The database variant to be employed is determined at runtime by the 
**config** package from an environment variable setting. 

If for example the database variant is specified as "neptune", the database
is constructed using `neptune.New()` and the returned object used as the 
**driver** in the `graph.DB` structure providing the external API. 

The `graph.DB`'s `driver` attribute type is only required to implement the 
(ultra minimal) `graph.driver.Driver` interface. 

However in our example use-case it must also satisfy the 
`graph.driver.CodeList` interface. The `config` package therefore checks that the
driver returned by `neptune.New()` also satisfies the `CodeList` interface as 
well - using a type assertion.

Note that `graph.DB` uses Go's embedded fields. For example the `graph.DB` struct 
contains an embedded `driver.CodeList`. It being "embedded" means that the
methods of CodeList can be accessed either through the implicit attribute 
name `CodeList`, **or** the shortcut form of a **promoted** method like 
this: `DB.GetCodeList()`.

Note also the way the config package sets all the DB attributes typified by
`DB.Codelist`, either to nil or to the (typecast) `config.Driver attribute`.
Which suggests these attributes differ, _but they do not_! If the database variant
in use supports say the `CodeList` interface and the `Hierarchy` inteface, and
both of these are selected, then both the `CodeList` attribute and the 
`Hierarchy` attribute will be set - but with the same Neptune driver object.
