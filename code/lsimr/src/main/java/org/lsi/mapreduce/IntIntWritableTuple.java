package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Sean
 */
public class IntIntWritableTuple implements Writable {
	
	//
	public Integer i;
	
	//
	public Integer parent;
	
	/**
	 * Constructor
	 * @param i
	 * @param parent
	 */
	public IntIntWritableTuple(Integer i, Integer parent){
		this.i = i;
		this.parent = parent;
	}

	/**
	 * Set the values.
	 * @param i
	 * @param parent
	 */
	public void set(Integer i, Integer parent){
		this.i = i;
		this.parent = parent;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.i = in.readInt();
		this.parent = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.i);
		out.writeInt(this.parent);
	}
}
