### hbase-rule
JUnit HBase rule which provides an embedded HBase server. It can be setup by custom configuration or default. The rule has also helper methods to work with hbase table and namespace such as create, delete, truncate and etc.

### Sample Usage

```
@ClassRule
public static HbaseRule hbaseRule = HbaseRule.newBuilder()
        .addNameSpace("builderNameSpace")
        .addTable("tableOnBuilder", "cf")
        .build();
        
@Test
public void test() throws Exception {
    try (Connection hbaseConnection = createConnection(hbaseRule.getZKAddress(), 3000);
            Table table = hbaseConnection.getTable(TableName.valueOf("tableOnBuilder"))) {

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
```
@BeforeClass
public static void setUp() throws Exception {
    hbaseRule.createTable("tableOnSetup", "cf");
}

@After
public void tearDown() throws Exception {
    hbaseRule.deleteTableData("tableOnSetup");
}

```

This rule provide several helper methods such as countRows(), deleteTable(), truncateTable() and etc.

```
@Test
public void test() throws Exception {
    // You expect number of rows already put in HBase table.
    Assert.assertEquals(/*Number of rows expected*/, hbaseRule.countRows("tableOnBuilder"));
    // Delete table data.
    hbaseRule.deleteTableData("tableOnBuilder");
    // You expect 0 rows in HBase table after cleaning.
    Assert.assertEquals(0, hbaseRule.countRows("tableOnBuilder"));
}
```

### Add it to your project
note:
The dependencies defined in the pom of this project, should be rewritten in the client pom. The user should define them with its appropriate choice of version. But he/she should note that he/she should define the server and minicluster dependencies in test scope.
You can reference to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from this jitpack link:
