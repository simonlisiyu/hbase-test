package com.lsy.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lisiyu on 2017/2/10.
 */
public class HBaseScanExample {
    public static void main(String[] args) {
        Map<String,Map> rowMap = new HashMap<>();    //存储Hbase表读出的值，map的k为rowId，v为column:value
        Map<String,Long> pidMap = new HashMap<>();      //存储租户+state为单位的计数值，map的k为 project_id:state，v为count
        long currnetTs = System.currentTimeMillis();
        String idc = "bj01";


        // hbase scan item为cpu_util的值，根据rowkey合并
        try {
            HTable hTable = HBaseClient.getHTable("meter");
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("counter_name"));
            scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("r_metadata.state"));
            scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("project_id"));
//            scan.setTimeRange(currnetTs-3600*1000L, currnetTs);
            scan.setTimeRange(1486709628000L, 1486709688000L);

            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("cpu_util"));
            scan.setFilter(filter);

            ResultScanner rs = hTable.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    String rowId = Bytes.toString(kv.getRow());
                    String family = Bytes.toString(kv.getFamily());
                    String column = Bytes.toString(kv.getQualifier());
                    String value = Bytes.toString(kv.getValue());
                    System.out.println("column="+column);
                    System.out.println("value="+value);
                    if(rowMap.containsKey(rowId)){
                        Map kvMap = rowMap.get(rowId);
                        kvMap.put(column, value);
                        rowMap.put(rowId,kvMap);
                    }else {
                        Map kvMap = new HashMap();
                        kvMap.put(column, value);
                        rowMap.put(rowId,kvMap);
                    }
                }
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 遍历rowmap，计算租户+state为单位的计数值
        for (Map.Entry<String,Map> entry : rowMap.entrySet()) {
            Map kvMap = entry.getValue();
            String projectId = kvMap.getOrDefault("project_id","").toString();
            String state = kvMap.getOrDefault("r_metadata.state","").toString();
            long count = pidMap.getOrDefault(projectId+":"+state, 0L);
            count++;
            pidMap.put(projectId+":"+state, count);
        }

        // 存到opentsdb中， metric为state，value为count，tag为projectId
        long currentTsSecond = currnetTs/1000;
        JSONArray array = new JSONArray();
        JSONObject metricJson = new JSONObject();
        try {
            metricJson.put("metric", "cloudhost.state.1h");
            metricJson.put("timestamp", currentTsSecond);

            for (Map.Entry<String,Long> entry : pidMap.entrySet()) {
                String projectId = entry.getKey().split(":")[0];
                String state = entry.getKey().split(":")[1];
                long value = entry.getValue();

                // make tags json
                JSONObject tagsJson = new JSONObject();
                tagsJson.put("project_id", projectId);
                tagsJson.put("state", state);
                tagsJson.put("idc", idc);

                // put metric json
                metricJson.put("value", value);
                metricJson.put("tags", tagsJson);

                array.put(metricJson);
            }
            System.out.println("array="+array);
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
