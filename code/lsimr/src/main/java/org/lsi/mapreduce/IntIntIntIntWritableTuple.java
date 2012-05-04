package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Sean
 */
public class IntIntIntIntWritableTuple implements Writable {
	public Integer groupidi;
	public Integer i;
    public Integer groupidp;
	public Integer parent;

	public void set(Integer groupidi, Integer i, Integer groupidp, Integer parent) {
		this.groupidi = groupidi;
		this.i = i;
        this.groupidp = groupidp;
		this.parent = parent;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.groupidi = in.readInt();
		this.i = in.readInt();
        this.groupidp = in.readInt();
		this.parent = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.groupidi);
		out.writeInt(this.i);
        out.writeInt(this.groupidp);
		out.writeInt(this.parent);
	}

	@Override
	public int hashCode() {
		return this.groupidi.hashCode() + this.i.hashCode()
				+ this.groupidp.hashCode() + this.parent.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntIntIntIntWritableTuple))
			return false;
		else {
			if (this.groupidi.equals(((IntIntIntIntWritableTuple) o).groupidi)
					&& this.i.equals(((IntIntIntIntWritableTuple) o).i)
                    && this.groupidp.equals(((IntIntIntIntWritableTuple) o).groupidp)
					&& this.parent.equals(((IntIntIntIntWritableTuple) o).parent))
				return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return this.groupidi+"_"+this.i+"_"+this.groupidp + "_" +this.parent;
	}
}
