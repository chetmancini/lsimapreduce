package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Sean
 */
public class IntIntIntWritableTuple implements Writable {
    public Integer groupid;
	public Integer i;
	public Integer parent;

	public void set(Integer groupid, Integer i, Integer parent){
        this.groupid = groupid;
		this.i = i;
		this.parent = parent;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
        this.groupid = in.readInt();
		this.i = in.readInt();
		this.parent = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
        out.writeInt(this.groupid);
		out.writeInt(this.i);
		out.writeInt(this.parent);
	}
}
