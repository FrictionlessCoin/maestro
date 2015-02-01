maestro
=======

[![Stories in Ready](https://badge.waffle.io/commbank/maestro.png?label=ready&title=Ready)](https://waffle.io/commbank/maestro) 
[![Build Status](https://travis-ci.org/CommBank/maestro.svg?branch=master)](https://travis-ci.org/CommBank/maestro)
[![Gitter chat](https://badges.gitter.im/CommBank/maestro.png)](https://gitter.im/CommBank/maestro)


```
maestro: a distinguished conductor
```

The `maestro` library provides convenient marshalling and
orchestration of data for ETL type work by providing a common framework
for conducting jobs combining a variety of data APIs.

The primary goal of `maestro` is to make it _easy_ to manage data sets
with out sacrificing safety or robustness. This is achieved by faithfully
sticking to strongly-typed schemas describing the fixed structure of
data, and providing APIs for manipulating those structures in a
sensible way that scales to data sets with 100s of columns.

[Scaladoc](https://commbank.github.io/maestro/latest/api/index.html)

__Handy links for related Scaladoc__

* omnia.maestro.api [package](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.api.package) and Maestro [object](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.api.Maestro$)
* omnia.parlour.[SqoopSyntax](http://commbank.github.io/parlour/latest/api/index.html#au.com.cba.omnia.parlour.SqoopSyntax$)
*  omnia.permafrost.hdfs.Hdfs [class](https://commbank.github.io/permafrost/latest/api/index.html#au.com.cba.omnia.permafrost.hdfs.Hdfs) and 
[object](https://commbank.github.io/permafrost/latest/api/index.html#au.com.cba.omnia.permafrost.hdfs.Hdfs$)
* omnia.ebenezer.scrooge.hive.Hive [class](https://commbank.github.io/ebenezer/latest/api/index.html#au.com.cba.omnia.ebenezer.scrooge.hive.Hive) and [object](https://commbank.github.io/ebenezer/latest/api/index.html#au.com.cba.omnia.ebenezer.scrooge.hive.Hive$)
* omnia.maestro.core.scalding [RichExecution class](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.core.scalding.RichExecution)
and [RichExecutionObject](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.core.scalding.RichExecutionObject)
* com.twitter.scalding.Execution [trait](http://twitter.github.io/scalding/index.html#com.twitter.scalding.Execution)
and [object](http://twitter.github.io/scalding/index.html#com.twitter.scalding.Execution$)


__Other__

* Github Pages: [https://commbank.github.io/maestro/index.html]()
* Extended documentation (in progress): [https://github.com/CommBank/maestro/tree/master/doc]()

starting point
--------------

`maestro` is designed to work with highly structured data. It is
expected that all data-sets manipulated by `maestro` at some level
(maybe input, output or intermediate representations) have a well
defined wide row schema and fixed set of columns.

At this point, `maestro` supports `thrift` for schema definitions.

`maestro` uses the thrift schema definition to derive as much meta-data and
implementation of custom processing (such as printing and parsing) as it
can. It then provides APIs that use these "data type" specific tools to
provide generic "tasks" like generating analytics views.


5 minute quick-start
--------------------

### Defining a thrift schema.

This is not the place for a full thrift tutorial, if you would like more
complete documentation then <http://diwakergupta.github.io/thrift-missing-guide/>
is a really good reference.

So if a dataset was going to land on the system, we would define a
schema accurately defining the columns and types:

```

#@namespace scala au.com.cba.omnia.etl.customer.thrift

struct Customer {
  1  : string CUSTOMER_ID
  2  : string CUSTOMER_NAME
  3  : string CUSTOMER_ACCT
  4  : string CUSTOMER_CAT
  5  : string CUSTOMER_SUB_CAT
  6  : i32 CUSTOMER_BALANCE
  7  : string EFFECTIVE_DATE
 }

```

This is a simplified example, a real data set may have 100s of
columns, but it should be enough to demonstrate. The important points
here are that _order_ is important, the struct should be defined to
have fields in the same order as input data, and _types_ are
important, they should accurately describe the data (and will be used
to infer how the data should be parsed and validated).


### Building a `maestro` job

The core of most ETL jobs can be implemented quickly and safely using
the features of `maestro`, but it is also designed to easily accommodate custom code, 
including code using raw APIs like scalding, hive, hdfs and sqoop.

A `maestro` job is defined via an `Execution` 
(see [scalding](https://github.com/twitter/scalding) and the `Execution` monad in the Concepts section below), 
generally involving multiple steps, with each step being an `Execution` itself and potentially 
depending on the results of previous steps.  This is often neatly expressed as a 
Scala `for`-`yield` comprehension like in the example below.

`maestro` includes convenient ways to construct `Execution` steps from
hive queries, sqoop import/export, hdfs operations, scalding pipes and
various other convenient ways of specifying operations and combining steps.

An example `maestro` job that loads data via hive is the 
[CustomerExecution](maestro-example/src/main/scala/au/com/cba/omnia/maestro/example/CustomerExecution.scala).
An extract follows to give the flavour of executions and the their configuration.

```scala
/** Configuration for a customer execution example */
case class CustomerConfig(config: Config)  {
  val maestro   = MaestroConfig(...)
  val upload    = maestro.upload()
  val load      = maestro.load[Customer](none="null")
  val dateTable = maestro.partitionedHiveTable[Customer, (String, String, String)] (...)
  val catTable  = maestro.partitionedHiveTable[Customer, String](...)
  val queries   = List(
    s"""INSERT OVERWRITE TABLE ${catTable.name} PARTITION (partition_cat)
      | SELECT id, name, acct, cat, sub_cat, -10, effective_date, cat AS partition_cat
      | FROM ${dateTable.name}""".stripMargin,
    s"""SELECT COUNT(*) FROM ${catTable.name}"""
  )
}

/** Customer execution example */
object CustomerJob extends MaestroJob {
  def job: Execution[JobStatus] = for {
    conf           <- Execution.getConfig.map(CustomerConfig(_))
    uploadInfo     <- upload(conf.upload)
    sources        <- uploadInfo.withSources
    (pipe, ldInfo) <- load[Customer](conf.load, uploadInfo.files)
    loadSuccess    <- ldInfo.withSuccess
    (count1, _)    <- viewHive(conf.dateTable, pipe) zip viewHive(conf.catTable, pipe)
    _              <- Execution.fromHive(Hive.queries(conf.queries), ...)
  } yield JobFinished
  ...
}
```

Concepts
--------

### Generated support for fields and records

`maestro` will use the metadata available from the thrift definition
to generate supporting infrastructure customized for _your_ specific
record type. This will give us the ability to refer to fields in code
for partitioning, validation, row filtering and transformations in a
way that can be easily be checked and validated up front (by the
compiler in most circumstances, and on start-up before things run in
the worst case).  Such field references can have 
interesting metadata which potentially allows us to automatically
parse, print, validate, filter, partition the data in a way that we
_know_ will work before we run the code (for a valid schema).

### Type Mappings

| Thrift Type                                       | Hive Type                                                                                     | Scala Type    |
| ------------------------------------------------- |:---------------------------------------------------------------------------------------------:| -------------:|
| bool: A boolean value (true or false), one byte   | BOOLEAN                                                                                       | bool          |
| byte: A signed byte                               | TINYINT (1-byte signed integer, from -128 to 127)                                             | byte          |
| i16: A 16-bit signed integer                      | SMALLINT (2-byte signed integer, from -32,768 to 32,767)                                      | short         |
| i32: A 32-bit signed integer                      | INT (4-byte signed integer, from -2,147,483,648 to 2,147,483,647)                             | int           |
| i64: A 64-bit signed integer                      | BIGINT (8-byte signed integer, from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)  | BigInteger    |
| double: A 64-bit floating point number            | DOUBLE (8-byte double precision floating point number)                                        | double        |
| string: Encoding agnostic text or binary string   | string 

### Execution monad
(See the scalding [`Execution` trait](http://twitter.github.io/scalding/#com.twitter.scalding.Execution) and [`Execution` object](http://twitter.github.io/scalding/#com.twitter.scalding.Execution$)
as well as the maestro extensions in [`RichExecution`](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.core.scalding.RichExecution))

An execution is an object with type `Execution[T]` representing some
work that can be performed that provides an item
of type `T` if it succeeds, otherwise it fails.  The work can depend
on configuration information, and can involve a variety of smaller
steps, includinghadoop jobs via scalding, with counters and caching
when appropriate.  Many small `Execution`s can be chained together
into a larger `Execution` using a `for`-`yield`-comprehension like in
the earlier example.

`for`-`yield`-comprehensions for `Execution[T]` have the same
structure as for other type constructors like `List[T]` and `Iterable[T]`.
This is because these type constructors are all monads, which roughly
means they allow chains to be formed via calls to `flatMap`.  Scala
`for`-`yield` comprehensions are actually just syntactic sugar for such chains, for
more details see this
[FAQ](http://docs.scala-lang.org/tutorials/FAQ/yield.html).


### Failures

Jobs should generally fail when unexpected conditions are detected so that appropriate action
can be taken to fix the situation.  Use `if(`condition`)` in a comprehension to cause
the whole `Execution` to fail with an error if the condition is false (via the
`filter` method, see the [FAQ](http://docs.scala-lang.org/tutorials/FAQ/yield.html)).
Exceptions in custom executions (see below) also lead to failures.

Some useful methods related to failures include
[`recoverWith`](http://twitter.github.io/scalding/#com.twitter.scalding.Execution),
[`bracket`](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.core.scalding.RichExecution)
[`ensuring`](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.core.scalding.RichExecution) and 
[`onException`](https://commbank.github.io/maestro/latest/api/index.html#au.com.cba.omnia.maestro.core.scalding.RichExecution).

### Custom executions

`Execution.from` allows any Scala code to be included in an `Execution`, however this should be done
carefully, including considering handling errors and other unusual situations.  If the code
throws an exception this is caught and converted into a failing `Execution`.

Pure expressions that just
return a value and never throw exceptions or perform effects should be included in comprehensions
using `x = ...` rather than `x <- Execution.from(...)`.

Hive and Hdfs operations should instead be included in `Execution`s via their own monads as outlined below. 

### Hive

Maestro allows you to write directly to Hive tables and to run queries on Hive tables as part of a
Maestro job.

`HiveTable` instances allow you to describe/reference a specific hive table.
The `source` and `sink` methods on the `HiveTable` provide Scalding
sources and sinks for typed pipes to read from or write to. `name` provides a fully qualifed name
that can be used inside hql.

`viewHive` allows the Maestro job to write out the data to a partitioned hive table in parquet
similar to `view`. However, it also creates the hive table if it doesn't already exist. Otherwise,
it just verifies the schema.

`Execution.fromHive` executes hive operations as part of a Maestro `Execution`, via the `Hive[T]` monad, providing an appropriate configuration from the `Execution` configuration.  The `Hive[T]` monad is similar to the `Execution[T]` monad but specifically for building hive operations that include steps like creating databases and tables, as well as performing queries (yielding lists of strings), and some additional support for checking conditions and building compound operations.

**Hive Limitations and issues**

* Currently it is not possible for our implementation to read in data from the partition columns.
  Instead it is expected that all the data is solely contained inside the core columns of the table
  itself. It is, therefore, not possible to partition on the same column as a field of the thrift
  struct (instead a duplicate column with a different name is required). Partition columns can only
  be used for hive performance reasons and not to carry information.

* In order for the job to work the hive-site.xml needs to be on the classpath when the job is
  initiated and on every node.

* Writing out hive files currently only works if the metastore is specified as thrift endpoint
  instead of database.
  ```
    <property>
      <name>hive.metastore.uris</name>
      <value>thrift://metastore:9083</value>
    </property>
    ```
* In order to run queries the hive-site.xml need to include the `yarn.resourcemanager.address`
  property even if the value is bogus.
  ```
    <property>
      <name>yarn.resourcemanager.address</name>
      <value>bogus</value>
    </property>
  ```
* In order to run queries with partitioning the partition mode needs to be set to nonstrict.
  ```
    <property>
      <name>hive.exec.dynamic.partition.mode</name>
      <value>nonstrict</value>
    </property>
  ```

You can start with the [example hive-site.xml](doc/hive-site.xml).  To use this either
install it on your cluster, or
add it to your project's resources directory so that it is included in your jar.



### Hdfs

Maestro provides support for Hdfs operations similar to the support for the `Hive[T]` monad
described above.

### Partitioners

Partitioners are really simple. Partitioners are just a list of fields to
partition a data set by.

The primary api is the list of fields you want to partition on:

```scala
Partition.byFields(Fields.CUSTOMER_CAT, Fields.CUSTOMER_SUB_CAT)
```

The api also has support for custom date formats, and
special support for dates of the `yyyy-MM-dd` form:

```scala
Partition.byDate(Fields.EFFECTIVE_DATE)
```

This will use that field, but split the partitioning into 3 parts of
yyyy, MM and dd.

### Validators

A Validator can be thought of as something that is a function from the record
type to either an error message or an "ok" tick of approval. In a lot of
cases this understanding can be simplified to saying it is a combination
of a `Field` to validate and a `Check` to apply. There are a few builtin
checks provided, if you want to do custom checking you can fail back to
defining a custom function.

```scala
Validator.all(
  Validator.of(fields.EFFECTIVE_DATE, Check.isDate),
  Validator.of(fields.CUSTOMER_CAT, Check.oneOf("BANK", "INSURE")),
  Validator.of(fields.CUSTOMER_NAME, Check.nonempty),
  Validator.of(fields.CUSTOMER_ID, Check.matches("\d+")),
  Validator.by[Customer](_.customerAcct.length == 4, "Customer accounts should always be a length of 4")
)
```

### Row filters, cleaners, splitters, transformers, macros

[TBD]
