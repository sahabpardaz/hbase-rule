package ir.sahab.hbaserule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Base class for creating an embeddable Hbase server.
 * It also provides some methods for easier access to the Hbase server.
 */
abstract class HbaseBase {

    Configuration configuration = HBaseConfiguration.create();
    HBaseTestingUtility utility;
    MiniZooKeeperCluster zkCluster;
    File logDir;

    List<String> nameSpaces = new ArrayList<>();
    List<HBaseTableDef> hBaseTableDefs = new ArrayList<>();

    HbaseBase() {
    }

    static Integer anOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Unable to find an open port.", e);
        }
    }

    public static Connection createConnection(String zkAddresses, int operationTimeout) throws IOException {
        // Create HBase client config
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkAddresses);
        hbaseConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, operationTimeout);
        return ConnectionFactory.createConnection(hbaseConf);
    }

    @SuppressWarnings("java:S5443")
    protected void before() throws Exception {
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
            try (final Stream<Path> files = Files.walk(Paths.get(logDir.getPath()))) {
                files.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
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

    /**
     * The model that represents definition of a Hbase table.
     */
    static class HBaseTableDef {

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

}
