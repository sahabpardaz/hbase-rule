package ir.sahab.hbaserule;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.rules.ExternalResource;

/**
 * JUnit rule which provides a mini cluster of Hbase, DFS, and ZooKeeper.
 */
public class HbaseRule extends ExternalResource {

    private Map<String, String> customConfigs;
    private HBaseTestingUtility utility;
    private MiniZooKeeperCluster zkCluster;
    private File logDir;

    private List<String> nameSpaces = new ArrayList<>();
    private List<HBaseTableDef> hBaseTableDefs = new ArrayList<>();

    public static Builder newBuilder() {
        return new Builder();
    }

    private HbaseRule() {
    }

    @Override
    protected void before() throws Throwable {
        utility = new HBaseTestingUtility();
        Configuration configuration = utility.getConfiguration();
        configuration.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
        if (customConfigs != null) {
            customConfigs.forEach(configuration::set);
        }
        // Set MASTER_INFO_PORT to a random open port instead of 60010 to avoid conflicts.
        configuration.set(HConstants.MASTER_INFO_PORT, String.valueOf(anOpenPort()));

        // Create ZooKeeper mini cluster
        int port = anOpenPort();
        zkCluster = new MiniZooKeeperCluster();
        zkCluster.addClientPort(port);
        logDir = Files.createTempDirectory("zk-log-dir").toFile();
        zkCluster.startup(logDir);

        // Create and start mini cluster
        utility.setZkCluster(zkCluster);
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost:" + port);
        utility.startMiniCluster();

        // Create defined namespaces and tables.
        for (String nameSpace : nameSpaces) {
            createNamespace(nameSpace);
        }

        for (HBaseTableDef hBaseTableDef : hBaseTableDefs) {
            utility.createTable(hBaseTableDef.getTableName(), hBaseTableDef.getColumnFamilies()).close();
        }
    }

    @Override
    protected void after() {
        try {
            utility.shutdownMiniCluster();
        } catch (Exception e) {
            throw new AssertionError("Failed to stop mini cluster.", e);
        }

        try {
            zkCluster.shutdown();
        } catch (IOException e) {
            throw new AssertionError("Failed to stop ZooKeeper cluster.", e);
        }

        try {
            Files.walk(Paths.get(logDir.getPath()))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new AssertionError("Failed to clear ZooKeeper log directory.", e);
        }
    }

    /**
     * It can be called after the rule is initialized. So you can call it in test method or in corresponding setup
     * method of the test, but not immediately after rule creation.
     */
    public String getZKAddress() {
        return utility.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
    }

    public Connection getConnection() throws IOException {
        return utility.getConnection();
    }

    public HBaseTestingUtility getHBaseTestingUtility() {
        return utility;
    }

    public Configuration getConfiguration() {
        return utility.getConfiguration();
    }

    public int countRows(TableName tableName) throws IOException {
        return utility.countRows(tableName);
    }

    public int countRows(String tableName) throws IOException {
        return countRows(TableName.valueOf(tableName));
    }

    public int countRows(Table table) throws IOException {
        return utility.countRows(table);
    }

    public int countRows(Table table, byte[]... columnFamilies) throws IOException {
        return utility.countRows(table, columnFamilies);
    }

    public void createNamespace(String nameSpace) throws IOException {
        utility.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(nameSpace).build());
    }

    public void createNamespace(NamespaceDescriptor namespace) throws IOException {
        utility.getHBaseAdmin().createNamespace(namespace);
    }

    public void createTable(String tableName, String columnFamily) throws IOException {
        utility.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily)).close();
    }

    public void createTable(String tableName, String columnFamily, int numVersions) throws IOException {
        utility.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily), numVersions).close();
    }

    public void createTable(byte[] tableName, byte[] columnFamily) throws IOException {
        utility.createTable(TableName.valueOf(tableName), columnFamily).close();
    }

    public void createTable(byte[] tableName, byte[] columnFamily, int numVersions) throws IOException {
        utility.createTable(TableName.valueOf(tableName), columnFamily, numVersions).close();
    }

    public void createTable(TableName tableName, byte[] columnFamily) throws IOException {
        utility.createTable(tableName, columnFamily).close();
    }

    public void createTable(TableName tableName, byte[] columnFamily, int numVersions) throws IOException {
        utility.createTable(tableName, columnFamily, numVersions).close();
    }

    public void createTable(TableName tableName, byte[]... columnFamilies) throws IOException {
        utility.createTable(tableName, columnFamilies).close();
    }

    public void createTable(TableName tableName, byte[][] columnFamilies, int[] numVersions) throws IOException {
        utility.createTable(tableName, columnFamilies, numVersions).close();
    }

    public void createTable(TableName tableName, String... columnFamilies) throws IOException {
        utility.createTable(tableName, columnFamilies).close();
    }

    public void deleteTable(String tableName) throws IOException {
        utility.deleteTable(tableName);
    }

    public void deleteTable(byte[] tableName) throws IOException {
        utility.deleteTable(tableName);
    }

    public void deleteTable(TableName tableName) throws IOException {
        utility.deleteTable(tableName);
    }

    public void deleteTableData(String tableName) throws IOException {
        deleteTableData(Bytes.toBytes(tableName));
    }

    public void deleteTableData(byte[] tableName) throws IOException {
        utility.deleteTableData(tableName).close();
    }

    public void deleteTableData(TableName tableName) throws IOException {
        utility.deleteTableData(tableName).close();
    }

    public void truncateTable(String tableName) throws IOException {
        truncateTable(Bytes.toBytes(tableName));
    }

    public void truncateTable(byte[] tableName) throws IOException {
        utility.truncateTable(tableName).close();
    }

    public void truncateTable(TableName tableName) throws IOException {
        utility.truncateTable(tableName).close();
    }

    static Integer anOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Unable to find an open port.", e);
        }
    }

    /**
     * The model that represents definition of a Hbase table.
     */
    private static class HBaseTableDef {

        private byte[] tableName;
        private byte[][] columnFamilies;

        public HBaseTableDef(byte[] tableName, byte[][] columnFamilies) {
            this.tableName = tableName;
            this.columnFamilies = columnFamilies;
        }

        public byte[] getTableName() {
            return tableName;
        }

        public byte[][] getColumnFamilies() {
            return columnFamilies;
        }
    }

    /**
     * The builder class to create instance of {@link HbaseRule}.
     */
    public static class Builder {

        HbaseRule hbaseRule = new HbaseRule();

        public Builder setCustomConfigs(Map<String, String> customConfigs) {
            hbaseRule.customConfigs = customConfigs;
            return this;
        }

        public Builder addNameSpace(String... nameSpaces) {
            hbaseRule.nameSpaces.addAll(Arrays.asList(nameSpaces));
            return this;
        }

        public Builder addNameSpace(byte[]... nameSpaces) {
            for (byte[] nameSpace : nameSpaces) {
                hbaseRule.nameSpaces.add(Bytes.toString(nameSpace));
            }
            return this;
        }

        public Builder addTable(byte[] tableName, byte[]... columnFamilies) {
            hbaseRule.hBaseTableDefs.add(new HBaseTableDef(tableName, columnFamilies));
            return this;
        }

        public Builder addTable(String tableName, String... columnFamilies) {
            byte[][] columnFamiliesBytes = new byte[columnFamilies.length][];
            for (int i = 0; i < columnFamilies.length; i++) {
                columnFamiliesBytes[i] = Bytes.toBytes(columnFamilies[i]);
            }
            hbaseRule.hBaseTableDefs.add(new HBaseTableDef(Bytes.toBytes(tableName), columnFamiliesBytes));
            return this;
        }

        public Builder addTable(TableName tableName, byte[]... columnFamilies) {
            hbaseRule.hBaseTableDefs.add(new HBaseTableDef(tableName.getName(), columnFamilies));
            return this;
        }

        public HbaseRule build() {
            return hbaseRule;
        }
    }
}