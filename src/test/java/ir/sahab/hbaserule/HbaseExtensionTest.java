package ir.sahab.hbaserule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HbaseExtensionTest {
    private static final String NAME_SPACE = "builderNameSpace";
    private static final String TABLE_ON_BUILDER = "tableOnBuilder";
    private static final String TABLE_ON_SETUP = "tableOnSetup";
    private static final String COLUMN_FAMILY = "cf";

    static HbaseExtension hbaseExtension = HbaseExtension.newBuilder().addNameSpace(NAME_SPACE).addTable(TABLE_ON_BUILDER, COLUMN_FAMILY).build();

    @BeforeAll
    static void setUp() throws Exception {
        hbaseExtension.createTable(TABLE_ON_SETUP, COLUMN_FAMILY);
    }

    static Connection createConnection(String zkAddresses, int operationTimeout) throws IOException {
        // Create HBase client config
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddresses);
        hbaseConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, operationTimeout);
        return ConnectionFactory.createConnection(hbaseConf);
    }

    @AfterEach
    void tearDown() throws Exception {
        hbaseExtension.deleteTableData(TABLE_ON_BUILDER);
        hbaseExtension.deleteTableData(TABLE_ON_SETUP);
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
        try (Connection hbaseConnection = createConnection(hbaseExtension.getZKAddress(), 3000); Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {

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
                    assertEquals(prefixRowKey + i, Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    assertEquals(prefixValue + i, Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        }

        // Test counting the rows
        assertEquals(numRecords, hbaseExtension.countRows(tableName));
    }

    private void checkTablesAreClean() throws Exception {
        assertEquals(0, hbaseExtension.countRows(TABLE_ON_BUILDER));
        assertEquals(0, hbaseExtension.countRows(TABLE_ON_SETUP));
    }

}
