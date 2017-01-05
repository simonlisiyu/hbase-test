package com.lsy.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lisiyu on 2016/12/19.
 * HTable的增删改查方法
 */
public class HTableCRUDMethod {
    static HTable table;
    static {
        try {
            table = HBaseClient.getHTable("test");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 验证是否存在
     * @param rowkey
     * @return true or false
     * @throws IOException
     */
    public static boolean exists(String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        return table.exists(get);
    }

    /**
     * 写入操作
     * put 单行写、多行写
     * checkAndPut 检查后写，原子操作
     * @param rowkey
     * @param cf
     * @param col1
     * @param value1
     * @param ts put的时间戳（optional）
     * @throws IOException
     */
    public static void put(String rowkey, String cf, String col1, long ts, String value1) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey)).add(Bytes.toBytes(cf), Bytes.toBytes(col1), ts, Bytes.toBytes(value1));
        table.put(put);
    }
    public static void put(String rowkey, String cf, String col1, String value1) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey)).add(Bytes.toBytes(cf), Bytes.toBytes(col1), Bytes.toBytes(value1));
        table.put(put);
    }
    public static void put(String rowkey, KeyValue kv) throws IOException {
//        KeyValue keyValue = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("col"), 100L, KeyValue.Type.Put, Bytes.toBytes("value"));
        Put put = new Put(Bytes.toBytes(rowkey)).add(kv);
        table.put(put);
    }
    public static void put(String[] rowkeys, String[] cfs, String[] cols, String[] values) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        for(int i=0;i<rowkeys.length;i++){
            Put put = new Put(Bytes.toBytes(rowkeys[i])).add(Bytes.toBytes(cfs[i]), Bytes.toBytes(cols[i]), Bytes.toBytes(values[i]));
            puts.add(put);
        }
        table.put(puts);
    }
    public static boolean checkAndPut(String rowkey, String cfCheck, String colCheck, String valueCheck, String cf, String col, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey)).add(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        return table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes(cfCheck), Bytes.toBytes(colCheck), Bytes.toBytes(valueCheck), put);
    }


    /**
     * 删除操作
     * delete 单行删、多行删
     * checkAndDelete 检查后删除，原子操作
     * @param rowkey
     * @throws IOException
     */
    public static void delete(String rowkey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
    }
    public static void delete(String[] rowkeys) throws IOException {
        List<Delete> deletes = new ArrayList<Delete>();
        for(int i=0;i<rowkeys.length;i++){
            Delete delete = new Delete(Bytes.toBytes(rowkeys[i]));
            deletes.add(delete);
        }
        table.delete(deletes);
    }
    public static boolean checkAndDelete(String rowkey, String cfCheck, String colCheck, String valueCheck) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        return table.checkAndDelete(Bytes.toBytes(rowkey), Bytes.toBytes(cfCheck), Bytes.toBytes(colCheck), Bytes.toBytes(valueCheck), delete);
    }

    /**
     * 更新（计数器）
     * increment 单列计数器更新加1
     * incrementColumnValue 多列计数器，更新一列指定值
     * @param rowkey
     * @return
     * @throws IOException
     */
    public static Result increment(String rowkey) throws IOException {
        return table.increment(new Increment(Bytes.toBytes(rowkey)));
    }
    public static long incrementColumnValue(String rowkey, String cf, String col, long amount) throws IOException {
        return table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(cf), Bytes.toBytes(col), amount);
    }
    public static long incrementColumnValue(String rowkey, String cf, String col, long amount, boolean writeToWAL) throws IOException {
        return table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(cf), Bytes.toBytes(col), amount, writeToWAL);
    }


    /**
     * 查询
     * get 单row查询、多row查询
     * getScanner 列族扫描、列扫描
     * @param rowkey
     * @return
     * @throws IOException
     */
    public static Result get(String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        return table.get(get);
    }
    public static Result[] get(String[] rowkeys) throws IOException {
        List<Get> gets = new ArrayList<Get>();
        for(String rowkey : rowkeys){
            Get get = new Get(Bytes.toBytes(rowkey));
            gets.add(get);
        }
        return table.get(gets);
    }
    public static ResultScanner getScanner(String cf) throws IOException {
        return table.getScanner(Bytes.toBytes(cf));
    }
    public static ResultScanner getScanner(String cf, String col) throws IOException {
        return table.getScanner(Bytes.toBytes(cf), Bytes.toBytes(col));
    }


}
