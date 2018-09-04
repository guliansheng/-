

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BlogAttention {
      public static class BlogMap extends Mapper<LongWritable,Text,Text,Text>{
    	  protected void map(LongWritable key,Text value,Context context) 
    			  throws IOException,InterruptedException{
    		  Text keyy = null;
    		  Text valuee = null;
    		  String[] strs = value.toString().split(" ");
    		  valuee = new Text(strs[0]);
    		  for(int i=1;i<strs.length;i++){
    			  for(int j=i+1;j<strs.length;j++){
    				  int is_true = strs[i].compareTo(strs[j]);
    				  if (is_true<=0){
    				     keyy = new Text(strs[i] + "_"+strs[j]);}
    				  else{
    					  keyy = new Text(strs[j] + "_"+strs[i]);
    				  }
    				  context.write(keyy,valuee);
    			  }
    		  }
    		  
    	  }
      }
      public static class BlogReduce extends Reducer<Text,Text,Text,Text>{
    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
    			  throws IOException,InterruptedException{
    		  StringBuffer sb = new StringBuffer();
    		  for(Text val:values){
    			  sb.append(val.toString() +" ");
    		  }
    		  sb.deleteCharAt(sb.length() - 1);
    		  Text value = new Text(sb.toString());
    		  context.write(key,value);
    		  
    	  }
    	 
      }
      public static void main(String[] args)throws Exception{
    	 
    	  Path input = new Path(args[0]);
    	  Path output = new Path(args[1]);
    	  Configuration conf = new Configuration();
    	  FileSystem fs = FileSystem.get(conf);
    	  fs.delete(output, true);
    	  Job job = new Job(conf);
    	  job.setJarByClass(BlogAttention.class);
    	  
    	  FileInputFormat.setInputPaths(job, input);
          FileOutputFormat.setOutputPath(job, output); 
          
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
          job.setMapperClass(BlogMap.class);
          job.setReducerClass(BlogReduce.class);
          job.waitForCompletion(true);
          
      }
}
