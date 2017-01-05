package com.lsy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lisiyu on 2016/12/8.
 */
public class HBaseClient {
    static Configuration hconf = HBaseConfiguration.create();
    static {
        hconf.set("hbase.zookeeper.quorum", "172.19.26.100,172.19.24.56,172.19.23.134");
        hconf.set("zookeeper.znode.parent", "/hbase/hbase_apollo_mjdos");
    }

    public static void main(String[] args) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hconf);
        try {
            if(admin.tableExists("meter")) {
                System.out.println(true);
            } else {
                System.out.println(false);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseAdmin getHAdmin() throws IOException {
        return new HBaseAdmin(hconf);
    }

    public static HTable getHTable(String tableName) throws IOException {
        return new HTable(hconf, Bytes.toBytes(tableName));
//        return new HTable(hconf, tableName);
    }



}
