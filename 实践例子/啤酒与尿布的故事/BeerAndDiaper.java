import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class BeerAndDiaper {
	
   public static  class BeerAndDiaperMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			
			
			
			String[] lists = value.toString().split(" ");
			Arrays.sort(lists);
			String keyy;
			IntWritable one = new IntWritable(1);
			for(int i=0;i<lists.length;i++){
				for(int j=i+1;j<lists.length;j++){
					
					keyy = lists[i]+"-"+lists[j];
					context.write(new Text(keyy), one);
					
				}
			}
		}
	}
	
	public static class BeerAndDiaperReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	    Set set = new HashSet();
	    int max = 0;
	    Text maxProduct = new Text();
	    protected void reduce(Text key,Iterable<IntWritable> values,Context context)
	     throws IOException,InterruptedException{
	    	
	    	String[] strs = key.toString().split("-");
	    	for(String str:strs){
	    		set.add(str);
	    	}
	    	int sum = 0;
	    	for(IntWritable t: values){
	    		sum++;
	    	}
	    	
//	    	if(sum>max){
//	    		max=sum;
//	    		maxProduct.set(key);
//	    	}
	    	context.write(key,new IntWritable(sum));
	    }
//		public void cleanup(Context context) throws IOException,InterruptedException{
//			context.write(new Text("All goods counts:"), new IntWritable(set.size()));
//			context.write(new Text (maxProduct+" Closely related ,meanwhile purchase counts:"),new IntWritable(max));
//		}
	}
	
	
	public static void main(String[] args) throws Exception{
		 
		if (args.length < 2) {  
		      System.err.println("Usage: wordcount <in> [<in>...] <out>");  
		      System.exit(2);  
		  }
		 Path input = new Path(args[0]);
		 Path output = new Path(args[1]);
		 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output);
		Job job = new Job(conf,"BeerAndDiaper");
		job.setJarByClass(BeerAndDiaper.class);
		
		
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job,output);
		
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(BeerAndDiaperMap.class);
		job.setReducerClass(BeerAndDiaperReduce.class);
		
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
