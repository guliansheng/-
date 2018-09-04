package training;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemIdPartitioner extends Partitioner<OrderBean,NullWritable>{
	
	public int getPartition(OrderBean bean,NullWritable value,int numReduceTasks){
		return (bean.getItemid().hashCode()&Integer.MAX_VALUE)%numReduceTasks;
	}

}
