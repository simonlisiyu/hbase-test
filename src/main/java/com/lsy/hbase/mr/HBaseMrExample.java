package com.lsy.hbase.mr;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

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

			String a = null;
			try {
				a = new String(value.getValue("f".getBytes(), "a".getBytes()));
				text.set(a);     // we can only emit Writables...
				context.write(text, ONE);
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}

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



			//添加一行记录，每一个单词作为行键
			Put put = new Put(Bytes.toBytes(key.toString()));
			//在列族result中添加一个标识符num,赋值为每个单词出现的次数
			//String.valueOf(sum)先将数字转化为字符串，否则存到数据库后会变成\x00\x00\x00\x这种形式
			//然后再转二进制存到hbase。
			put.add(Bytes.toBytes("f"), Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(sum)));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}

	public static void main(String[] args) throws Exception {

		HBaseConfiguration conf = new HBaseConfiguration();
		Job job = new Job(conf, "HBaseMrExample");
		job.setJarByClass(HBaseMrExample.class);

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs


		TableMapReduceUtil.initTableMapperJob(
				"test",      // input table
				scan,             // Scan instance to control CF and attribute selection
				MyMapper.class,   // mapper class
				Text.class,         // mapper output key
				IntWritable.class,  // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				"t1",        // output table
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
