import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;


public class Test {
   public static void main(String[] args) {
	   
	   /*System.out.println("Hello Word");*/
	 
	   Configuration conf = new Configuration();
	   
  
	     // 获取所有节点
	     DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
	     
	     try {
	         // 返回FileSystem对象
	         FileSystem fs = FileSystem.get(conf);
	         
	         // 获取分布式文件系统
	         DistributedFileSystem hdfs = (DistributedFileSystem)fs;
	         
	         dataNodeStats = hdfs.getDataNodeStats();
	     } catch (IOException e) {
	       
	     }
	    for(DatanodeInfo aa:dataNodeStats){
	    	
	    	System.out.println(aa.getParent());
	    	}
	    }
	 
}
