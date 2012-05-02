package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Sean
 */
public class IntIntWritableTuple implements Writable {
	Integer l;
	Integer parent;

	public void set(Integer l, Integer parent){
		this.l = l;
		this.parent = parent;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		l = in.readLong();
		parent = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(l);
		out.writeLong(parent);
	}
}
