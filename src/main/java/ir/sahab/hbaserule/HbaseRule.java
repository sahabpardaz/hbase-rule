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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.rules.ExternalResource;

/**
 * JUnit rule which provides a mini cluster of Hbase, DFS, and ZooKeeper.
 */
public class HbaseRule extends ExternalResource {

    private Configuration configuration = HBaseConfiguration.create();
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
        // We disable info servers to increase the speed of work and prevent contention for ports.
        // This approach has even been used in the HBase tests too.
        // Note: In the configuration properties, default value for {@link HConstants.MASTER_PORT} and
        //       {@link HConstants#REGIONSERVER_PORT} and {@link HConstants#REGIONSERVER_INFO_PORT} are
        //       overridden with 0 so a random port will be assigned (Based on the implementation of
        //       {@link HttpServer}, HBase starts from a specific port and increment it by 1 until finds
        //       a free port)
        //       However HBase mini cluster doesn't override {@link HConstants#MASTER_INFO_PORT}!!
        //       (See {@link LocalHBaseCluster#Constructor(Configuration configuration)})
        // Note: {@link HConstants#REGIONSERVER_INFO_PORT} and {@link HConstants#MASTER_INFO_PORT} are
        //       for Hadoop web UIs. It is possible to disable info severs by providing -1 as port.
        //       (See {@link HRegionServer#putUpWebUI})
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

        public Builder setCustomConfig(String name, String value) {
            hbaseRule.configuration.set(name, value);
            return this;
        }

        public Builder setCustomConfig(String name, String... values) {
            hbaseRule.configuration.setStrings(name, values);
            return this;
        }

        public Builder setCustomConfigs(Map<String, String> customConfigs) {
            customConfigs.forEach(hbaseRule.configuration::set);
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