package com.lsy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Created by lisiyu on 2016/12/19.
 * HTable的获取元数据信息和状态信息管理的方法
 */
public class HTableMetaAndStatusMethod {
    static HTable table;
    static {
        try {
            table = HBaseClient.getHTable("test");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * getRegionLocation 获取rowkey所在Region的位置信息
     * getRegionLocations 获取所有Region的位置信息
     * @param rowkey
     * @return
     * @throws IOException
     */
    public static HRegionLocation getRegionLocation(String rowkey) throws IOException {
        return table.getRegionLocation(rowkey);
    }
    public static HRegionLocation getRegionLocation(String rowkey, boolean reload) throws IOException {
        return table.getRegionLocation(Bytes.toBytes(rowkey), reload);
    }
    public static NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
        return table.getRegionLocations();
    }

    /**
     * getConfiguration 获取客户端配置信息
     * getOperationTimeout 获取操作超时时间
     * @return
     */
    public static Configuration getConfiguration(){
        return table.getConfiguration();
    }
    public static int getOperationTimeout(){
        return table.getOperationTimeout();
    }

    /**
     * getStartKey 获取开始行健
     * getEndKeys 获取结束行健
     * @return
     * @throws IOException
     */
    public static byte[][] getStartKeys() throws IOException {
        return table.getStartKeys();
    }
    public static byte[][] getEndKeys() throws IOException {
        return table.getEndKeys();
    }

    /**
     * isAutoFlush 是否自动Flush到磁盘
     * isTableEnabled 表状态是否可用
     * @return
     */
    public static boolean isAutoFlush(){
        return table.isAutoFlush();
    }
    public static boolean isTableEnabled(Configuration conf, String tableName) throws IOException {
        return table.isTableEnabled(conf, Bytes.toBytes(tableName));
    }

    /**
     * setAutoFlush 设置自动Flush到磁盘、Flush失败时是否清除客户端写Buffer
     * flushCommits 即时提交Flush到磁盘
     * setOperationTimeout 设置操作超时时间
     * setWriteBufferSize 设置客户端写缓存大小
     * @param autoFlush
     */
    public static void setAutoFlush(boolean autoFlush){
        table.setAutoFlush(autoFlush);
    }
    public static void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail){
        table.setAutoFlush(autoFlush, clearBufferOnFail);
    }
    public static void flushCommits() throws IOException {
        table.flushCommits();
    }
    public static void setOperationTimeout(int operationTimeout){
        table.setOperationTimeout(operationTimeout);
    }
    public static void setWriteBufferSize(long writeBufferSize) throws IOException {
        table.setWriteBufferSize(writeBufferSize);
    }
}
