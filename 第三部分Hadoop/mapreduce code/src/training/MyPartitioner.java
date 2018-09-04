package training;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text,FlowWritable> {
	
	
//	public static HashMap<String,Integer> proviceDict = new HashMap<String,Integer>();
//	static{
//		proviceDict.put("137",0);
//		proviceDict.put("133",1);
//		proviceDict.put("138",2);
//		proviceDict.put("135",3);
//	}
	public int getPartition(Text key,FlowWritable value,int reduceNum){
		
		
        //String prefix = key.toString().substring(0,3);
//		Integer provinceId = proviceDict.get(prefix);
//		return provinceId;
		if(key.toString().contains("137")){
			return 0;
		}else if(key.toString().contains("133")){
			return 1;
		}else if(key.toString().contains("138")){
			return 2;
		}else if(key.toString().contains("135")){
			return 3;
		}else{
			return 4;
		}
		
	}

}
