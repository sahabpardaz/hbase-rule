# hbase-rule
JUnit rule which provides an embedded HBase server. It can be setup by custom configuration or default. The rule has also helper methods to work with hbase table and namespace such as create, delete, truncate and etc.

# Sample Usage

```
private static final String NAME_SPACE = "builderNameSpace";
private static final String TABLE_ON_BUILDER = "tableOnBuilder";
private static final String TABLE_ON_SETUP = "tableOnSetup";
private static final String COLUMN_FAMILY = "cf";

@ClassRule
public static HbaseRule hbaseRule = HbaseRule.newBuilder()
        .addNameSpace(NAME_SPACE)
        .addTable(TABLE_ON_BUILDER, COLUMN_FAMILY)
        .build();

@BeforeClass
public static void setUp() throws Exception {
    hbaseRule.createTable(TABLE_ON_SETUP, COLUMN_FAMILY);
}

@After
public void tearDown() throws Exception {
    hbaseRule.deleteTableData(TABLE_ON_BUILDER);
    hbaseRule.deleteTableData(TABLE_ON_SETUP);
}

@Test
public void readAndWrite() throws Exception {

    try (Connection hbaseConnection = createConnection(hbaseRule.getZKAddress(), 3000);
            Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_ON_SETUP /*or TABLE_ON_BUILDER*/))) {

        String rowKey = "rowKey";
        String value = "value";
        String qualifier = "q";

        // Put some data to hbase table.
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);

        // Get date from Hbase table and validate them.
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            Assert.assertEquals(rowKey, Bytes.toString(cell.getRow()));
            Assert.assertEquals(value, Bytes.toString(cell.getValue()));
        }
    }

    // Test counting the rows
    Assert.assertEquals(1, hbaseRule.countRows(TABLE_ON_SETUP /*or TABLE_ON_BUILDER*/));
}
```
