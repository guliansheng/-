package training;



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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeyWord {
      public static class KeywordMap extends Mapper<LongWritable,Text,Text,Text>{
    	  Text keyWord;
    	  Text keyValue = new Text("1");
    	  protected void map(LongWritable key,Text value,Context context) 
    			  throws IOException,InterruptedException{
    		  
    		 FileSplit split = (FileSplit)context.getInputSplit();
    		 String fileName = split.getPath().getName();
    		 String valueStr = value.toString().replaceAll(","," ");
    		  String[] strs = valueStr.toString().split(" ");
    		 
    		  for(int i=0;i<strs.length;i++){
    			  keyWord = new Text(strs[i].toLowerCase()+"_"+fileName);
    				  context.write(keyWord,keyValue);}
    		  } 
    	}
      
      
      public static class keywordCombiner extends Reducer<Text,Text,Text,Text>{
    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
    			  throws IOException,InterruptedException{
    		  String[] keyWord = key.toString().split("_");
    		  String word = keyWord[0];
    		  String fileName = keyWord[1];
    		  int sum = 0;
    		  for(Text it:values){
    			  sum+= Integer.parseInt(it.toString());
    		  }
    		  
    		  context.write(new Text(word),new Text(fileName +":"+sum));
    		  
    	  }
    	 
      }
      
//      public static class KeywordReduce extends Reducer<Text,Text,Text,Text>{
//    	  protected void reduce(Text key,Iterable<Text> values,Context context) 
//    			  throws IOException,InterruptedException{
//    		  StringBuffer sb = new StringBuffer();
//    		  for(Text val:values){
//    			  sb.append(val.toString());
//    			  sb.append(",");
//    		  }
//    		  sb.deleteCharAt(sb.length() - 1);
//    		  Text value = new Text(sb.toString());
//    		  context.write(key,value);
//    		  
//    	  }
//    	 
//      }

      public static void main(String[] args)throws Exception{
    	 
    	  Path input0 = new Path(args[0]);
    	  Path input1 = new Path(args[1]);
    	  Path input2 = new Path(args[2]);
    	  Path output = new Path(args[3]);
    	  Configuration conf = new Configuration();
    	  FileSystem fs = FileSystem.get(conf);
    	  fs.delete(output, true);
    	  Job job = new Job(conf);
    	  job.setJarByClass(KeyWord.class);
    	  
    	  FileInputFormat.setInputPaths(job, input0,input1,input2);
          FileOutputFormat.setOutputPath(job, output); 
          
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
          job.setMapperClass(KeywordMap.class);
          //job.setCombinerClass(keywordCombiner.class);
          job.setReducerClass(keywordCombiner.class);
          job.waitForCompletion(true);
          
      }
}
