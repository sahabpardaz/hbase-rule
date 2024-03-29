### HBase Rule

[![Tests](https://github.com/sahabpardaz/hbase-rule/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/sahabpardaz/hbase-rule/actions/workflows/maven.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=coverage)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=vulnerabilities)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=security_rating)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=sqale_index)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_hbase-rule&metric=alert_status)](https://sonarcloud.io/dashboard?id=sahabpardaz_hbase-rule)
[![JitPack](https://jitpack.io/v/sahabpardaz/hbase-rule.svg)](https://jitpack.io/#sahabpardaz/hbase-rule)

This library provides Junit 4 Rule and Junit 5 Extension which provides an embedded HBase server.
It can be setup by custom configuration or default.
The rule and extension has also helper methods to work with hbase table and namespace such as create,
delete, truncate and etc.

### Sample Usage

```java
@ClassRule
public static HbaseRule hbaseRule = HbaseRule.newBuilder()
        .addNameSpace("namespace")
        .addTable("table", "cf")
        .build();

@Test
public void test() throws Exception {
    try (Connection hbaseConnection = createConnection(hbaseRule.getZKAddress(), 3000);
            Table table = hbaseConnection.getTable(TableName.valueOf("table"))) {

        String rowKey = "rowKey";
        String value = "value";
        String qualifier = "q";

        // Put some data to HBase table.
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);

        // Get data from HBase table and validate it.
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            Assert.assertEquals(rowKey, Bytes.toString(cell.getRow()));
            Assert.assertEquals(value, Bytes.toString(cell.getValue()));
        }
    }
}
```

It's also possible to define table on setup and delete the data on tearDown.

```java
@BeforeClass
public static void setUp() throws Exception {
    hbaseRule.createTable("table", "cf");
}

@After
public void tearDown() throws Exception {
    hbaseRule.deleteTableData("table");
}

```

This rule provide several helper methods such as countRows(), deleteTable(), truncateTable() and etc.

```java
@Test
public void test() throws Exception {
    ...

    // You expect number of rows already put in HBase table.
    Assert.assertEquals(10 , hbaseRule.countRows("tableOnBuilder"));
    // Delete table data.
    hbaseRule.deleteTableData("table");
    // You expect 0 rows in HBase table after cleaning.
    Assert.assertEquals(0, hbaseRule.countRows("table"));
}
```

## JUnit 5 Support

In case of using Junit 5, You can use HbaseExtension extension like this:

```java
@RegisterExtension
static HbaseExtension hbaseExtension = HbaseExtension.newBuilder()
        .addNameSpace("namespace")
        .addTable("table", "cf")
        .build();

@BeforeAll
static void setUp() throws Exception {
    hbaseExtension.createTable("table", "cf");
}

@Test
void test() throws Exception {
        ...

        // You expect number of rows already put in HBase table.
        assertEquals(10 , hbaseExtension.countRows("tableOnBuilder"));
        // Delete table data.
        hbaseExtension.deleteTableData("table");
        // You expect 0 rows in HBase table after cleaning.
        assertEquals(0, hbaseExtension.countRows("table"));
        }
```

### Add it to your project

You can reference to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from
this jitpack link:
[![](https://jitpack.io/v/sahabpardaz/hbase-rule.svg)](https://jitpack.io/#sahabpardaz/hbase-rule)

But note that you should rewrite all optional dependencies defined in [pom](pom.xml) in pom of your own project too.
That's because here we have defined dependencies as optional to avoid accidantally changing the type (*original* or *
cloudera*, *normal* or *shaded*) and version of your hadoop dependencies.

If you have used hadoop client libraries, you have already some hadoop dependencies in your project. Now you should
provide some other dependencies for hadoop *server* and *test* libraries. Note that you should provide them with the
same type and version but in *test* scope.

JUnit 4 and 5 dependencies are marked as optional, so you need to provide JUnit 4 or 5 dependency
(based on what version you need, and you use) in you project to make it work.
