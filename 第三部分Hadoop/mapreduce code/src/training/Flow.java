package training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Flow {
	public static class FMaper extends Mapper<LongWritable,Text,Text,FlowWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] strs = value.toString().split("\\|");
			String phoneId = strs[0];
			long upFlow = Long.parseLong(strs[1]);
			long dFlow =Long.parseLong(strs[2]);
			//long totFlow = upFlow + dFlow;
			FlowWritable fW = new FlowWritable(upFlow,dFlow);
			context.write(new Text(phoneId), fW);
		}
	}
	
	public static class FReduce extends Reducer<Text,FlowWritable,Text,FlowWritable>{
		
		public void reduce(Text key,Iterable<FlowWritable> value,Context context) throws IOException,InterruptedException{
			
		   // String[] str;
		    
		    long upFlow=0;
		    long dFlow=0;
		    long totFlow=0;
		    
			for(FlowWritable val:value){
				//str=val.toString().split(",");
				upFlow=upFlow+val.getupFlow();
				dFlow = dFlow +val.getdFlow();
				totFlow = upFlow+dFlow;
			}
			FlowWritable fs = new FlowWritable(upFlow,dFlow);
			//context.write(key, new Text(upFlow+","+dFlow+","+totFlow));
			context.write(key,fs);
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
			job.setJobName("Flow");
			job.setJarByClass(Flow.class);
			
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, OutputPath);
			
			job.setMapperClass(FMaper.class);
			job.setReducerClass(FReduce.class);
			//job.setCombinerClass(WReducer.class);
			
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setNumReduceTasks(4);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowWritable.class);
			job.setPartitionerClass(MyPartitioner.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FlowWritable.class);
			
			job.waitForCompletion(true);
			
		
	}

}
