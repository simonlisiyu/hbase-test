package com.lsy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap;

import java.io.IOException;

/**
 * Created by lisiyu on 2016/12/20.
 * HBase的HTable连接池类
 * 多线程、写入时线程安全的HTable的连接池，无须单独维护HTable对象
 * 老方法，已经弃用
 */
public class HBaseHTablePool {
    static Configuration hconf = HBaseConfiguration.create();
    static {
        hconf.set("hbase.zookeeper.quorum", "172.19.26.100,172.19.24.56,172.19.23.134");
        hconf.set("zookeeper.znode.parent", "/hbase/hbase_apollo_mjdos");
    }

    public static HTablePool getHTablePool(){
        return new HTablePool(hconf, Integer.MAX_VALUE);
    }
    public static HTablePool getHTablePool(int maxSize){
        return new HTablePool(hconf, maxSize);
    }
    public static HTablePool getHTablePool(int maxSize, HTableFactory factory, PoolMap.PoolType type){
        return new HTablePool(hconf, maxSize, factory, type);
    }
    /**
     * PoolMap.PoolType.Reusable    底层使用ConcurrnetLinkedQueue实现
     * PoolMap.PoolType.RoundRobin  无法使用
     * PoolMap.PoolType.ThreadLocal 底层使用ThreadLocal实现，每个线程维护资金独有的变量拷贝，以空间换取线程安全，访问性能更高
     */





    public static void main(String[] args) {
        HTablePool pool = getHTablePool();
        Result result = null;
        HTable table = null;
        try{
            table = (HTable)pool.getTable("test");
            if(table == null) throw new RuntimeException("This table is not exist!");
            result = table.get(new Get(Bytes.toBytes("rk1")));
            System.out.println("result="+result.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(table != null){
                try {
                    pool.putTable((HTableInterface)table);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
