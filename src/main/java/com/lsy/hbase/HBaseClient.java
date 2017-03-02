package com.lsy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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
        hconf.set("hbase.zookeeper.quorum", "172.27.53.72");
//        hconf.set("zookeeper.znode.parent", "/hbase/hbase_apollo_mjdos");
    }

    public static void main(String[] args) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hconf);
        try {
            if(admin.tableExists("meter")) {
                System.out.println(true);
            } else {
                System.out.println(false);
            }

            HTable table = getHTable("meter");
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("counter_name"));
            scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("r_metadata.state"));
            scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("project_id"));
            scan.setTimeRange(1486709088000L, 1486709688000L);
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("cpu_util"));
            scan.setFilter(filter);
            ResultScanner rs =  table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.raw()) {
                    System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
                            Bytes.toString(kv.getRow()),
                            Bytes.toString(kv.getFamily()),
                            Bytes.toString(kv.getQualifier()),
                            Bytes.toString(kv.getValue()),
                            kv.getTimestamp()));
                }
            }

            rs.close();
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
