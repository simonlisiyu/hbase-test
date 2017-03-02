package com.lsy.hbase.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.lsy.hbase.HBaseClient;
import com.lsy.hbase.commons.HttpUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class HBaseMrExample {

	/**
	 * TableMapper<Text,IntWritable>  Text:输出的key类型，IntWritable：输出的value类型
	 */
	public static class MyMapper extends TableMapper<Text,IntWritable>{

		private static IntWritable ONE = new IntWritable(1);
		private static Text text = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
//			String rowkey = new String(value.getRow());
			String state = new String(value.getValue("f".getBytes(), "r_metadata.state".getBytes()));
			String projectId = new String(value.getValue("f".getBytes(), "project_id".getBytes()));
			text.set(projectId+":"+state);     // we can only emit Writables...
			context.write(text, ONE);
		}
	}

	/**
	 * TableReducer<Text,IntWritable>  Text:输入的key类型，IntWritable：输入的value类型，ImmutableBytesWritable：输出类型
	 */
	public static class MyReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			for(IntWritable val:values) {
				sum+=val.get();
			}
			JSONObject metricJson = new JSONObject();
			try {
				metricJson.put("metric", "cloudhost.state.1h");
				metricJson.put("timestamp", Long.parseLong(context.getConfiguration().get("timestamp")));

				JSONObject tagsJson = new JSONObject();
				tagsJson.put("project_id", key.toString().split(":")[0]);
				tagsJson.put("state", key.toString().split(":")[1]);
				tagsJson.put("idc", context.getConfiguration().get("idc"));

				metricJson.put("value", sum);
				metricJson.put("tags", tagsJson);

				HttpUtils.doPost("http://172.27.36.77:4242/api/put", metricJson.toString());
			} catch (JSONException e) {
				e.printStackTrace();
			}


//			//添加一行记录，每一个单词作为行键
//			Put put = new Put(Bytes.toBytes(key.toString()));
//			//在列族result中添加一个标识符num,赋值为每个单词出现的次数
//			//String.valueOf(sum)先将数字转化为字符串，否则存到数据库后会变成\x00\x00\x00\x这种形式
//			//然后再转二进制存到hbase。
//			put.add(Bytes.toBytes("result"), Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(sum)));
//			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.err.println("Error: Parameter input is incorrect!");
			System.err.println("Usage: HBaseMrExample <User ID> <Book Set Name>");
			System.exit(-1);
		}

		String idc = args[0];

		HBaseConfiguration conf = new HBaseConfiguration();
		Job job = new Job(conf, "HBaseMrExample");
		job.setJarByClass(HBaseMrExample.class);

		job.getConfiguration().set("timestamp", System.currentTimeMillis()+"");
		job.getConfiguration().set("idc", idc);


		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("counter_name"));
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("r_metadata.state"));
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("project_id"));
//            scan.setTimeRange(currnetTs-3600*1000L, currnetTs);
		scan.setTimeRange(1486709628000L, 1486709688000L);
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("cpu_util"));
		scan.setFilter(filter);


		TableMapReduceUtil.initTableMapperJob(
				"meter",      // input table
				scan,             // Scan instance to control CF and attribute selection
				MyMapper.class,   // mapper class
				Text.class,         // mapper output key
				IntWritable.class,  // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				"meter",        // output table
				MyReducer.class,    // reducer class
				job);
		job.setNumReduceTasks(10);   // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			System.exit(1);
			throw new IOException("error with job!");
		} else {
			System.exit(0);
		}
//		System.exit(job.waitForCompletion(true)?0:1);
	}

}
