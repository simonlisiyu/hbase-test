package com.lsy.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by lisiyu on 2016/12/19.
 * HBaseAdmin的创建、修改、删除表方法
 */
public class HBaseAdminCRUDMethod {
    static HBaseAdmin admin;
    static {
        try {
            admin = HBaseClient.getHAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * createTable 表和列族、表和列族和region分隔字符、表和列族和key范围和初始region数
     * createTableAsync 异步创建表，表和列族和region分隔字符
     * @param tableName
     * @param cf
     * @throws IOException
     */
    public static void createTable(String tableName, String cf) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        admin.createTable(hTableDescriptor);
    }
    public static void createTable(String tableName, String[] cfs) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf : cfs){
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(hTableDescriptor);
    }
    public static void createTable(String tableName, String cf, byte[][] splitKeys) throws IOException {
        /*byte[][] splitKeys = new byte[][] {
            Bytes.toBytes("A"),
            Bytes.toBytes("D"),
            Bytes.toBytes("G"),
            Bytes.toBytes("K"),
            Bytes.toBytes("O"),
            Bytes.toBytes("T")
         };*/
        /*
        [1] start key: , end key: A
        [2] start key: A, end key: D
        [3] start key: D, end key: G
        [4] start key: G, end key: K
        [5] start key: K, end key: O
        [6] start key: O, end key: T
        [7] start key: T, end key:
        * */
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        admin.createTable(hTableDescriptor, splitKeys);
    }
    public static void createTable(String tableName, String cf, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        admin.createTable(hTableDescriptor, startKey, endKey, numRegions);
    }
    public static void createTableAsync(String tableName, String cf, byte[][] splitKeys) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        admin.createTableAsync(hTableDescriptor, splitKeys);
    }

    /**
     * 删除表和列的方法
     * deleteTable 删除表
     * deleteTables 删除多表，正则匹配
     * deleteColumn 删除表的列
     * deleteSnapshot 删除快照
     * @param tableName
     * @param col
     * @throws IOException
     */
    public static void deleteColumn(String tableName, String col) throws IOException {
        admin.deleteColumn(tableName, col);
    }
    public static void deleteSnapshot(String snapshotName) throws IOException {
        admin.deleteSnapshot(snapshotName);
    }
    public static void deleteTable(String tableName) throws IOException {
        admin.deleteTable(tableName);
    }
    public static HTableDescriptor[] deleteTables(String regex) throws IOException {
        return admin.deleteTables(regex);
    }

    /**
     * 修改表
     * addColumn 增加一个列族
     * modifyColumn 修改表的列族
     * modifyTable 修改表的列族一个、多个
     * @param tableName
     * @param cf
     * @throws IOException
     */
    public static void addColumn(String tableName, String cf) throws IOException {
        admin.addColumn(tableName, new HColumnDescriptor(cf));
    }
    public static void modifyColumn(String tableName, String cf) throws IOException {
        admin.modifyColumn(tableName, new HColumnDescriptor(cf));
    }
    public static void modifyTable(String tableName, String cf) throws IOException {
        admin.modifyTable(tableName, new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(cf)));
    }
    public static void modifyTable(String tableName, String[] cfs) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf : cfs){
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        admin.modifyTable(tableName, hTableDescriptor);
    }
}
