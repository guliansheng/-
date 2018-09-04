package training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import training.WordCount.WMapper;
import training.WordCount.WReducer;

public class GroupSort {
	public static class FMaper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
		
		OrderBean bean = new OrderBean();
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] strs = value.toString().split("\\|");
			if (strs.length==3){
			bean.set(new Text(strs[0]), new DoubleWritable(Double.parseDouble(strs[2])));
			
			context.write(bean, NullWritable.get());
			}
		}
	}
	
	public static class FReduce extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
		
		public void reduce(OrderBean key,Iterable<NullWritable> value,Context context) throws IOException,InterruptedException{
			
		  
		   
			context.write(key,NullWritable.get());
		}
	}
	
	public static void  main(String[] args) throws Exception{
			if(args.length<2){
				System.out.println("Usage:<inputPath><outputPath>");
				System.exit(0);
			}
			Path inputPath = new Path(args[0]);
			Path OutputPath = new Path(args[1]);
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			if(fs.exists(OutputPath)){
				fs.delete(OutputPath,true);
			}
			
			Job job = Job.getInstance(conf);
			job.setJobName("GroupSort");
			job.setJarByClass(GroupSort.class);
			
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, OutputPath);
			
			job.setMapperClass(FMaper.class);
			job.setReducerClass(FReduce.class);
			
			job.setNumReduceTasks(4);
			job.setMapOutputKeyClass(OrderBean.class);
			job.setMapOutputValueClass(NullWritable.class);
			//job.setPartitionerClass(MyPartitioner.class);
			job.setOutputKeyClass(OrderBean.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setPartitionerClass(ItemIdPartitioner.class);
			
			job.setGroupingComparatorClass(MyGroupingComparator.class);
			
			job.waitForCompletion(true);
			
		
	}

}
