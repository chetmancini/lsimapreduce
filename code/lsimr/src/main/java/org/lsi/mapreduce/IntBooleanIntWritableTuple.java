package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntBooleanIntWritableTuple implements Writable{
	Integer i;
	Boolean b;
	Integer parent;

	public void set(int i, boolean b, int parent){
		this.i = i;
		this.b = b;
		this.parent = parent;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		b = in.readBoolean();
		parent = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeBoolean(b);
		out.writeInt(parent);
	}
}
