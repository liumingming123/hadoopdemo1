package com.test.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by liusong on 2017/6/15.
 */
public class FlowSum {
	public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		Text ip=new Text();
		LongWritable one=new LongWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String []split=line.split(" ");
			if(split[0]!=null) {
				ip.set(split[0]);
			}
			context.write(ip,one);
		}
	}
	public static  class MyPartitoner extends Partitioner<Text,LongWritable>{
		@Override
		public int getPartition(Text text, LongWritable longWritable, int i) {
			int result=0;
			if(text.toString().length()==11){
				result=1;
			}else if(text.toString().length()==12){
				result=2;
			}else if(text.toString().length()>=13){
				result=3;
			}else{
				result=0;
			}
			return result;
		}
	}
	public static  class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		LongWritable suml=new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum=0;
			for(LongWritable s:values){
				sum+=s.get();
			}
			suml.set(sum);
			context.write(key,suml);

		}
	}
	public static  void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(FlowSum.class);
		job.setNumReduceTasks(4);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitoner.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}
}
