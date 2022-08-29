package ir.sahab.hbaserule;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Map;

/**
 * Builder class for building {@link HbaseExtension} and {@link HbaseRule}
 *
 * @param <T> {@link HbaseExtension} or {@link HbaseRule}
 */
class Builder<T extends HbaseBase> {


    T hbase;

    Builder(T initial) {
        hbase = initial;
    }

    public Builder<T> setCustomConfig(String name, String value) {
        hbase.configuration.set(name, value);
        return this;
    }

    public Builder<T> setCustomConfig(String name, String... values) {
        hbase.configuration.setStrings(name, values);
        return this;
    }

    public Builder<T> setCustomConfigs(Map<String, String> customConfigs) {
        customConfigs.forEach(hbase.configuration::set);
        return this;
    }

    public Builder<T> addNameSpace(String... nameSpaces) {
        hbase.nameSpaces.addAll(Arrays.asList(nameSpaces));
        return this;
    }

    public Builder<T> addNameSpace(byte[]... nameSpaces) {
        for (byte[] nameSpace : nameSpaces) {
            hbase.nameSpaces.add(Bytes.toString(nameSpace));
        }
        return this;
    }

    public Builder<T> addTable(byte[] tableName, byte[]... columnFamilies) {
        hbase.hBaseTableDefs.add(new HbaseBase.HBaseTableDef(tableName, columnFamilies));
        return this;
    }

    public Builder<T> addTable(String tableName, String... columnFamilies) {
        byte[][] columnFamiliesBytes = new byte[columnFamilies.length][];
        for (int i = 0; i < columnFamilies.length; i++) {
            columnFamiliesBytes[i] = Bytes.toBytes(columnFamilies[i]);
        }
        hbase.hBaseTableDefs.add(new HbaseBase.HBaseTableDef(Bytes.toBytes(tableName), columnFamiliesBytes));
        return this;
    }

    public Builder<T> addTable(TableName tableName, byte[]... columnFamilies) {
        hbase.hBaseTableDefs.add(new HbaseBase.HBaseTableDef(tableName.getName(), columnFamilies));
        return this;
    }

    public T build() {
        return hbase;
    }


}
