package ir.sahab.hbaserule;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class HbaseRuleTest {

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
    public void testTableCreatedByBuilder() throws Exception {
        checkTablesAreClean();
        checkWriteAndRead(TABLE_ON_BUILDER);
    }

    @Test
    public void testTableCreatedDirectly() throws Exception {
        checkTablesAreClean();
        checkWriteAndRead(TABLE_ON_SETUP);
    }

    private void checkWriteAndRead(String tableName) throws Exception {

        int numRecords = 100;
        try (Connection hbaseConnection = createConnection(hbaseRule.getZKAddress(), 3000);
                Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {

            // Put some data to hbase table.
            String prefixRowKey = "rowKey";
            String prefixValue = "value";
            String qualifier = "p";
            for (int i = 0; i < numRecords; i++) {
                Put put = new Put(Bytes.toBytes(prefixRowKey + i));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(qualifier), Bytes.toBytes(prefixValue + i));
                table.put(put);
            }

            // Get date from Hbase table and validate them.
            for (int i = 0; i < numRecords; i++) {
                Get get = new Get(Bytes.toBytes(prefixRowKey + i));
                get.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(qualifier));
                Result result = table.get(get);
                for (Cell cell : result.listCells()) {
                    Assert.assertEquals(prefixRowKey + i, Bytes.toString(cell.getRow()));
                    Assert.assertEquals(prefixValue + i, Bytes.toString(cell.getValue()));
                }
            }
        }

        // Test counting the rows
        Assert.assertEquals(numRecords, hbaseRule.countRows(tableName));
    }

    static Connection createConnection(String zkAddresses, int operationTimeout) throws IOException {
        // Create HBase client config
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddresses);
        hbaseConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, operationTimeout);
        return ConnectionFactory.createConnection(hbaseConf);
    }

    private void checkTablesAreClean() throws Exception {
        Assert.assertEquals(0, hbaseRule.countRows(TABLE_ON_BUILDER));
        Assert.assertEquals(0, hbaseRule.countRows(TABLE_ON_SETUP));
    }
}
