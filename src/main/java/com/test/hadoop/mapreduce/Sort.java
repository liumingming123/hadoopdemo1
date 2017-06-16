package com.test.hadoop.mapreduce;

import java.io.IOException;

import com.test.hadoop.po.MySort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Sort {
	public static class SortMapper extends Mapper<LongWritable,Text,MySort,LongWritable>{
		MySort ms=new MySort();
		LongWritable t1=new LongWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] sp=line.split("\t");
			ms.setText(sp[0]);
			ms.setNum(Integer.parseInt(sp[1]));
			t1.set(Integer.parseInt(sp[1]));
			context.write(ms,t1);
		}
	}
	public static class SortReducer extends Reducer<MySort,LongWritable,Text,LongWritable>{
		LongWritable t1=new LongWritable();
		Text text=new Text();
		@Override
		protected void reduce(MySort key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			t1.set(key.getNum());
			text.set(key.getText());
			context.write(text,t1);
		}
	}
	public static  void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setMapOutputKeyClass(MySort.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}

}
