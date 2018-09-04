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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	
	public static class WMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void Map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			
			String[] strs = value.toString().split(" ");
			Text keyS = new Text("");
			IntWritable values = new IntWritable(0);
			for(String str:strs){
				keyS.set(str);
				values.set(1);
				context.write(keyS,values);
			}
		}
	}
	public static class WReducer extends Reducer<Text,IntWritable,NullWritable,Text>{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException,InterruptedException{
			int sum=0;
			Text values = new Text(" ");
			for(IntWritable num:value){
				sum=sum+num.get();
			}
			String ss=key.toString()+" "+sum;
			values.set(ss);
			context.write(NullWritable.get(), values);
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
		job.setJobName("WorldCount");
		job.setJarByClass(WordCount.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, OutputPath);
		
		job.setMapperClass(WMapper.class);
		job.setReducerClass(WReducer.class);
		//job.setCombinerClass(WReducer.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		
		
	}

}
