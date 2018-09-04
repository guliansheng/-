

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Trip {
	  int number=0;
      public static class TripMap extends Mapper<LongWritable,Text,Text,Text>{
    	 
    	  protected void map(LongWritable key,Text value,Context context) 
    			  throws IOException,InterruptedException{
    		// FileSplit split = (FileSplit)context.getInputSplit();
    		// String fileName = split.getPath().getName();
    		// String valueStr = value.toString().replaceAll(",","");
    		  String[] strs = value.toString().split("\\|");
    		  context.write(new Text(strs[2]),new Text(strs[7]+" "+strs[8]+" "+strs[9]));}
    		  
    	}
      
      
     
      
      public static class TripReduce extends Reducer<Text,Text,Text,Text>{
    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
    			  throws IOException,InterruptedException{
    		  //StringBuffer sb = new StringBuffer();
    		  int sum0=0;
    		  int sum1=0;
    		  int sum2=0;
    		  String[] strs=null;
    		  for(Text val:values){
    			strs=val.toString().split(" ");
    			sum0=sum0+Integer.parseInt(strs[0]);
    			sum1=sum1+Integer.parseInt(strs[1]);
    			sum2=sum2+Integer.parseInt(strs[2]);
    			
    		  }
    		 
    		  
    		  context.write(key,new Text(String.valueOf(sum0)+" "+String.valueOf(sum1)+" "+String.valueOf(sum2)));
    		  
    	  }
    	 
      }

      public static void main(String[] args)throws Exception{
    	 
    	  Path input = new Path(args[0]);
    	  Path output = new Path(args[1]);
    	  
    	  Configuration conf = new Configuration();
    	  FileSystem fs = FileSystem.get(conf);
    	  fs.delete(output, true);
    	  Job job = new Job(conf);
    	  job.setJarByClass(Trip.class);
    	  
    	  FileInputFormat.setInputPaths(job, input);
          FileOutputFormat.setOutputPath(job, output); 
          
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
          job.setMapperClass(TripMap.class);
          //job.setCombinerClass(TripCombiner.class);
          job.setReducerClass(TripReduce.class);
          job.waitForCompletion(true);
          
      }
}
