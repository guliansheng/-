
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
import org.slf4j.Logger;  
import org.slf4j.LoggerFactory;

public class UserAttention {
	  private static Logger logger=LoggerFactory.getLogger(UserAttention.class); 
      public static class UserMap extends Mapper<LongWritable,Text,Text,Text>{
    	  protected void map(LongWritable key,Text value,Context context) 
    			  throws IOException,InterruptedException{
    		  String[] strs = value.toString().split(" ");
    		 
    		  logger.debug("xxketest"+value.toString());
    		  for(int i=1;i<strs.length;i++){
    			  context.write(new Text(strs[i]),new Text(strs[0]));
    		  }
    		  
    	  }
      }
      public static class UserReduce extends Reducer<Text,Text,NullWritable,Text>{
    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
    			  throws IOException,InterruptedException{
    		  StringBuffer sb = new StringBuffer();
    		  
    		  //int sum =0;
    		  for(Text val:values){
    			  sb.append(val.toString() +" ");
    			  //sum++;
    		  }
    		  sb.deleteCharAt(sb.length() - 1);
    		  Text value = new Text(key.toString()+" "+sb.toString());
    		  context.write(NullWritable.get(),value);
    		  //context.write(new Text(key.toString()+"_count"),new Text(Integer.toString(sum)));
    		  
    	  }
    	 
      }
      public static void main(String[] args)throws Exception{
    	 
    	  Path input = new Path(args[0]);
    	  Path output = new Path(args[1]);
    	  Configuration conf = new Configuration();
    	  FileSystem fs = FileSystem.get(conf);
    	  fs.delete(output, true);
    	  Job job = new Job(conf);
    	  job.setJarByClass(UserAttention.class);
    	  
    	  FileInputFormat.setInputPaths(job, input);
          FileOutputFormat.setOutputPath(job, output); 
          
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
          job.setMapperClass(UserMap.class);
          job.setReducerClass(UserReduce.class);
          job.waitForCompletion(true);
          
      }
}
