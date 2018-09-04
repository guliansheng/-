package training;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupingComparator extends WritableComparator{
	
	public MyGroupingComparator(){
		
		super(OrderBean.class,true);
	}
	
	
	public int compare(WritableComparable a,WritableComparable b){
		OrderBean ob1 = (OrderBean)a;
		OrderBean ob2 = (OrderBean)b;
		return ob1.getItemid().compareTo(ob2.getItemid());
	}

}
