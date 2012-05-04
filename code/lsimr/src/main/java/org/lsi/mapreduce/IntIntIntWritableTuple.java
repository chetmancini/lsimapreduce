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

	public void set(Integer groupid, Integer i, Integer parent) {
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

	@Override
	public int hashCode() {
		return this.groupid.hashCode() + this.i.hashCode()
				+ this.parent.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntIntIntWritableTuple))
			return false;
		else {
			if (this.groupid.equals(((IntIntIntWritableTuple) o).groupid)
					&& this.i.equals(((IntIntIntWritableTuple) o).i)
					&& this.parent.equals(((IntIntIntWritableTuple) o).parent))
				return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return this.groupid+"_"+this.i+"_"+this.parent;
	}
}
