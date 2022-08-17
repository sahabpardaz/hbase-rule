package ir.sahab.hbaserule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.*;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class HbaseExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    public static final TestInstance.Lifecycle lifecycle = TestInstance.Lifecycle.PER_CLASS;

    private final Configuration configuration = HBaseConfiguration.create();
    private final List<String> nameSpaces = new ArrayList<>();
    private final List<HBaseTableDef> hBaseTableDefs = new ArrayList<>();
    private HBaseTestingUtility utility;
    private MiniZooKeeperCluster zkCluster;
    private File logDir;

    private HbaseExtension() {
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    static Integer anOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Unable to find an open port.", e);
        }
    }

    private void setup() throws Exception {
        // Create ZooKeeper mini cluster
        logDir = Files.createTempDirectory("zk-log-dir").toFile();
        zkCluster = new MiniZooKeeperCluster();
        int zkPort = anOpenPort();
        zkCluster.addClientPort(zkPort);
        zkCluster.startup(logDir);
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost:" + zkPort);

        // Create and start HBase mini cluster
        utility = new HBaseTestingUtility(configuration);
        utility.setZkCluster(zkCluster);
        configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
        // We disable info servers for two reasons:
        // 1. In tests, we do not need UI dashboards. So we can ignore it and speed up the launch time.
        // 2. When we assign an open port for these servers, because there is a delay involved (on starting
        //    the HBase mini-cluster, it first launches data nodes and region servers), the port may be used
        //    by another application and no longer available.
        configuration.setInt(HConstants.MASTER_INFO_PORT, -1);
        configuration.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
        utility.startMiniCluster();

        // Create defined namespaces and tables.
        for (String nameSpace : nameSpaces) {
            createNamespace(nameSpace);
        }

        for (HBaseTableDef hBaseTableDef : hBaseTableDefs) {
            utility.createTable(hBaseTableDef.getTableName(), hBaseTableDef.getColumnFamilies()).close();
        }
    }

    private void teardown() {
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
            Files.walk(Paths.get(logDir.getPath())).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
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

    public MiniDFSCluster getDfsCluster() {
        return utility.getDFSCluster();
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

    @Override
    public void afterAll(ExtensionContext context) {
        if (lifecycle == TestInstance.Lifecycle.PER_CLASS) {
            teardown();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (lifecycle == TestInstance.Lifecycle.PER_METHOD) {
            teardown();
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (lifecycle == TestInstance.Lifecycle.PER_CLASS) {
            setup();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (lifecycle == TestInstance.Lifecycle.PER_METHOD) {
            setup();
        }
    }

    /**
     * The model that represents definition of a Hbase table.
     */
    private static class HBaseTableDef {

        private final byte[] tableName;
        private final byte[][] columnFamilies;

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

        HbaseExtension hbaseExtension = new HbaseExtension();

        public Builder setCustomConfig(String name, String value) {
            hbaseExtension.configuration.set(name, value);
            return this;
        }

        public Builder setCustomConfig(String name, String... values) {
            hbaseExtension.configuration.setStrings(name, values);
            return this;
        }

        public Builder setCustomConfigs(Map<String, String> customConfigs) {
            customConfigs.forEach(hbaseExtension.configuration::set);
            return this;
        }

        public Builder addNameSpace(String... nameSpaces) {
            hbaseExtension.nameSpaces.addAll(Arrays.asList(nameSpaces));
            return this;
        }

        public Builder addNameSpace(byte[]... nameSpaces) {
            for (byte[] nameSpace : nameSpaces) {
                hbaseExtension.nameSpaces.add(Bytes.toString(nameSpace));
            }
            return this;
        }

        public Builder addTable(byte[] tableName, byte[]... columnFamilies) {
            hbaseExtension.hBaseTableDefs.add(new HBaseTableDef(tableName, columnFamilies));
            return this;
        }

        public Builder addTable(String tableName, String... columnFamilies) {
            byte[][] columnFamiliesBytes = new byte[columnFamilies.length][];
            for (int i = 0; i < columnFamilies.length; i++) {
                columnFamiliesBytes[i] = Bytes.toBytes(columnFamilies[i]);
            }
            hbaseExtension.hBaseTableDefs.add(new HBaseTableDef(Bytes.toBytes(tableName), columnFamiliesBytes));
            return this;
        }

        public Builder addTable(TableName tableName, byte[]... columnFamilies) {
            hbaseExtension.hBaseTableDefs.add(new HBaseTableDef(tableName.getName(), columnFamilies));
            return this;
        }

        public HbaseExtension build() {
            return hbaseExtension;
        }
    }
}
