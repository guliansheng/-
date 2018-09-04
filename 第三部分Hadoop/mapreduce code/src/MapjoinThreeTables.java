import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 *
 */
public class MapjoinThreeTables {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MapjoinThreeTables.class);
		job.setMapperClass(MapjoinThreeTables_Mapper.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		String inputpath = args[0];
		String outpath = args[1];
		//两个小文件地址
		URI uri1 = new URI(args[2]);
		URI uri2 = new URI(args[3]);
		job.addCacheFile(uri1);//不能漏掉！！！
		job.addCacheFile(uri2);
		Path inputPath = new Path(inputpath);
		Path outputPath = new Path(outpath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isdone = job.waitForCompletion(true);
		System.exit(isdone ? 0 : 1);
	}
	
	public static class MapjoinThreeTables_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private static Map<String, String> moivemap = new HashMap<>();
		private static Map<String, String> usersmap = new HashMap<>();
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			
			Path[] paths = context.getLocalCacheFiles();
			//1::Toy Story (1995)::Animation|Children's|Comedy
			//通过地址读取电影数据
			String strmoive = paths[0].toUri().toString();
			BufferedReader bf1 = new BufferedReader(new FileReader(new File(strmoive)));
			String stringLine = null;
			while((stringLine = bf1.readLine()) != null){
				String[] reads = stringLine.split("::");
				String moiveid = reads[0];
				String moiveInfo = reads[1] + "::" + reads[2];
				moivemap.put(moiveid, moiveInfo);
			}
			//1::F::1::10::48067
			//通过地址读取用户数据
			String struser = paths[1].toUri().toString();
			BufferedReader bf2 = new BufferedReader(new FileReader(new File(struser)));
			String stringLine2 = null;
			while((stringLine2 = bf2.readLine()) != null){
				String[] reads = stringLine2.split("::");
				String userid = reads[0];
				String userInfo = reads[1] + "::" + reads[2] + "::" + reads[3] + "::" + reads[4];
				usersmap.put(userid, userInfo);
			}
			//关闭资源
			IOUtils.closeStream(bf1);
			IOUtils.closeStream(bf2);

		}
		Text kout = new Text();
		Text valueout = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String [] reads1 = value.toString().trim().split("::");
			//1::1193::5::978300760 :用户ID，电影ID，评分，评分时间戳
			//通过电影id和用户id在对应的map中获取信息，正常来说通过表的外键设置，ratings不存在空信息，如果存在空信息，需要进行map.contain判断
			String struser = usersmap.get(reads1[0]);
			String strmoive = moivemap.get(reads1[1]);
			//进行三表拼接，数据格式：userid, movieId, rate, ts, gender, age, occupation, zipcode, movieName, movieType
			String [] userinfo = struser.split("::");//gender, age, occupation, zipcode
			String [] moiveinfo = strmoive.split("::");//movieName, movieType
			String kk = reads1[0] + "::" + reads1[1] + "::" + reads1[2] + "::" + reads1[3] + "::" 
						+ userinfo[0] + "::" + userinfo[1] + "::" + userinfo[2] + "::" + userinfo[3] + "::"
						+ moiveinfo[0] + "::" + moiveinfo[1];
			kout.set(kk);
			context.write(kout, NullWritable.get());

		}
	}


}