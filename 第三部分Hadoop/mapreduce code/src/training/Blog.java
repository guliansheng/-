package training;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Blog {
	
	
    static final String INPUT_PATH = "/data/input/blog";
    static final String OUT_PATH = "/data/output/blog";
    
	public static class FMaper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			  
			String[] strs=value.toString().split(" ");
			int length=0;
			String topic=strs[0];
			Text keyy = new Text("") ;
			Text valuee = new Text(topic);

			for(int i=1;i<strs.length;i++){
				keyy.set(strs[i]);
				context.write(keyy, valuee);
			}
		}
	}
	
	public static class FReduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			//{A,(1,2,4,6)}
			String result=new String();
			int time=0;
			for(Text val:value){
				result=result+" "+val;
				time++;
			}
		result=result+" "+"total:"+time;
			context.write(key, new Text(result));
		}
	}
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inPath = new Path(INPUT_PATH);
        FileSystem fileSystem = FileSystem.get(conf);
        Path outPath = new Path(OUT_PATH);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
		Job job = Job.getInstance(conf);
		job.setJobName("homework");
		job.setJarByClass(Blog.class);
        //job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
        job.setMapperClass(FMaper.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(LongWritable.class);
        //job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
        //job.setNumReduceTasks(1);
        
        //job.setReducerClass(FReduce.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        
        //job.setCombinerClass(MyReducer.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}