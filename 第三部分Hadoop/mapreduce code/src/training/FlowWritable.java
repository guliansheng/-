package training;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowWritable implements Writable{
	
	private long upFlow;
	private long dFlow;
	private long totFlow;
	
	public FlowWritable(){
		
	}
	
	public FlowWritable(long upFlow,long dFlow){
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.totFlow = upFlow + dFlow;
	}
	
	
	public long getupFlow(){
		
		return this.upFlow;
	}
	
	public void setupFlow(long upFlow){
		
		this.upFlow = upFlow;	
	}
	
    public long getdFlow(){
		
		return this.dFlow;
	}
	public void setdFlow(long dFlow){
		
		this.dFlow = dFlow;
		
	}
	public void readFields(DataInput in) throws IOException{
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeLong(upFlow);
		out.writeLong(dFlow);
	}
	
	public String toString(){
		return this.upFlow+":"+this.dFlow+":"+totFlow;
	}

}
