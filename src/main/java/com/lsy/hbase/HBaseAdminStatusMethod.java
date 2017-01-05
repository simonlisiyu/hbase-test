package com.lsy.hbase;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by lisiyu on 2016/12/19.
 * HBaseAdmin的状态、快照、其他信息管理
 */
public class HBaseAdminStatusMethod {
    static HBaseAdmin admin;
    static {
        try {
            admin = HBaseClient.getHAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下线表
     * disableTable 下线表
     * disableTableAsync 异步下线表
     * disableTables 下线多表（正则匹配）
     * @param tableName
     * @throws IOException
     */
    public static void disableTable(String tableName) throws IOException {
        admin.disableTable(tableName);
    }
    public static void disableTableAsync(String tableName) throws IOException {
        admin.disableTableAsync(tableName);
    }
    public static HTableDescriptor[] disableTables(String regex) throws IOException {
        return admin.disableTables(regex);
    }

    /**
     * 上线表
     * enableTable 上线表
     * enableTableAsync 异步上线表
     * enableTables 上线多表（正则匹配）
     * @param tableName
     * @throws IOException
     */
    public static void enableTable(String tableName) throws IOException {
        admin.enableTable(tableName);
    }
    public static void enableTableAsync(String tableName) throws IOException {
        admin.enableTableAsync(tableName);
    }
    public static HTableDescriptor[] enableTables(String regex) throws IOException {
        return admin.enableTables(regex);
    }

    /**
     * isMasterRunning 集群的master是否活跃
     * isTableDisabled 表是否被禁用
     * isTableEnabled 表是否被启用
     * @return
     * @throws IOException
     */
    public static boolean isMasterRunning() throws IOException {
        return admin.isMasterRunning();
    }
    public static boolean isTableDisabled(String tableName) throws IOException {
        return admin.isTableDisabled(tableName);
    }
    public static boolean isTableEnabled(String tableName) throws IOException {
        return admin.isTableEnabled(tableName);
    }

    /**
     * getTableDescriptor 获取表描述
     * getTableDescriptors 获取多表描述
     * listTables 列出所有表名、根据正则列出匹配的表名
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HTableDescriptor getTableDescriptor(String tableName) throws IOException {
        return admin.getTableDescriptor(Bytes.toBytes(tableName));
    }
    public static HTableDescriptor[] getTableDescriptors(List<String> tableNames) throws IOException {
        return admin.getTableDescriptors(tableNames);
    }
    public static HTableDescriptor[] listTables() throws IOException {
        return admin.listTables();
    }
    public static HTableDescriptor[] listTables(String regex) throws IOException {
        return admin.listTables(regex);
    }


    /**
     * getClusterStatus 获取集群信息
     * getMasterCoprocessors 获取协处理器信息
     * flush 将内存中的数据序列化到硬盘上，根据表名或region名
     * @return
     * @throws IOException
     */
    public static ClusterStatus getClusterStatus() throws IOException {
        return admin.getClusterStatus();
    }
    public static String[] getMasterCoprocessors() throws IOException {
        return admin.getMasterCoprocessors();
    }
    public static void flush(String tableNameOrRegionName) throws IOException, InterruptedException {
        admin.flush(tableNameOrRegionName);
    }

    /**
     * 生成快照 根据快照名、表名、快照类型
     * @param snapshotName
     * @param tableName
     * @throws IOException
     */
    public static void snapshot(String snapshotName, String tableName) throws IOException {
        admin.snapshot(snapshotName, tableName);
    }
    public static void snapshot(String snapshotName, String tableName, HBaseProtos.SnapshotDescription.Type type) throws IOException {
        admin.snapshot(snapshotName, tableName, type);
    }


}
