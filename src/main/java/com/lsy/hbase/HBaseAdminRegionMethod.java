package com.lsy.hbase;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by lisiyu on 2016/12/19.
 * HBaseAdmin的Region操作方法
 */
public class HBaseAdminRegionMethod {
    static HBaseAdmin admin;
    static {
        try {
            admin = HBaseClient.getHAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * getTableRegions 获取表的region信息
     * balancer 即时负载均衡region
     * move 移动region到指定regionserver
     * @param tableName
     * @return
     * @throws IOException
     */
    public static List<HRegionInfo> getTableRegions(String tableName) throws IOException {
        return admin.getTableRegions(Bytes.toBytes(tableName));
    }
    public static void balancer() throws IOException {
        admin.balancer();
    }
    public static void move(String encodedRegionName, String destServerName) throws IOException {
        admin.move(Bytes.toBytes(encodedRegionName), Bytes.toBytes(destServerName));
    }

    /**
     * compact  minor-compact合并region，根据表名、region名、列族名
     * majorCompact major-compact合并region，根据表名、region名、列族名(慎用)
     * split 分裂region，根据表名、region名、分裂点
     * @param tableNameOrRegionName
     * @throws IOException
     */
    public static void compact(String tableNameOrRegionName) throws IOException {
        admin.compact(tableNameOrRegionName);
    }
    public static void compact(String tableNameOrRegionName, String cf) throws IOException {
        admin.compact(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(cf));
    }
    public static void majorCompact(String tableNameOrRegionName) throws IOException {
        admin.majorCompact(tableNameOrRegionName);
    }
    public static void majorCompact(String tableNameOrRegionName, String cf) throws IOException {
        admin.majorCompact(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(cf));
    }
    public static void split(String tableNameOrRegionName) throws IOException, InterruptedException {
        admin.split(tableNameOrRegionName);
    }
    public static void split(String tableNameOrRegionName, String splitPoint) throws IOException {
        admin.split(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(splitPoint));
    }

    /**
     * 管理Region到当前管理的regionserver
     * assign 分配
     * unassign 解除分配（之后会将此region随机分配到一个regionserver，有可能分配回到当前regionserver）
     * @param regionName
     * @throws IOException
     */
    public static void assign(String regionName) throws IOException {
        admin.assign(Bytes.toBytes(regionName));
    }
    public static void unassign(String regionName, boolean force) throws IOException {
        admin.unassign(Bytes.toBytes(regionName), force);
    }

    /**
     * closeRegion 关闭regionserver上的region
     * closeRegionWithEncodedRegionName 关闭regionserver上的region（region name is encoded）
     * @param regionName
     * @param regionServer
     * @throws IOException
     */
    public static void closeRegion(String regionName, String regionServer) throws IOException {
        admin.closeRegion(regionName, regionServer);
    }
    public static boolean closeRegionWithEncodedRegionName(String encodedRegionName, String regionServer) throws IOException {
        return admin.closeRegionWithEncodedRegionName(encodedRegionName, regionServer);
    }



}
