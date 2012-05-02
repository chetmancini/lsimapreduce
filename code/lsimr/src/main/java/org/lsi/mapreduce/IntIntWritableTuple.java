package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntIntWritableTuple implements Writable{
	Integer i;
	Integer parent;

	public void set(int i, int parent){
		this.i = i;
		this.parent = parent;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		parent = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeInt(parent);
	}
}
