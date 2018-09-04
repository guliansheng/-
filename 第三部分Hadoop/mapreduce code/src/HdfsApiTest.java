import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;




public class HdfsApiTest {
	public static void main(String[] args)throws IOException{
		
		if(args.length < 2){
			System.out.println("Usage HdfsApiTest<S_path><D_path>");
			System.exit(0);
		}
		Path s_path = new Path(args[0]);
		Path d_path = new Path(args[1]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		 
        
//		FSDataInputStream fsin = fs.open(s_path);
//		byte[] buff = new byte[128];
//		//long ln = 0;
//		int length = 0;
//		while((length = fsin.read(buff,0,128)) !=-1){
//			System.out.println(new String(buff,0,length-1));
//			//System.out.println(fsin.getPos());
//			
//		}
//		
		
		FSDataOutputStream fsout = fs.create(s_path);
		byte[] buff = "I am love you\n".getBytes();
		fsout.write(buff);
		IOUtils.closeQuietly(fsout);
	    fs.close();
		
		
	}

}
