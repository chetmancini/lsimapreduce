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
	public IntIntWritableTuple(){
		this.i = -1;
		this.parent = -1;
	}
	
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
	
	@Override
	public int hashCode() {
		return this.i.hashCode()
				+ this.parent.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntIntWritableTuple))
			return false;
		else {
			if (this.i.equals(((IntIntWritableTuple) o).i)
					&& this.parent.equals(((IntIntWritableTuple) o).parent))
				return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return this.i+"_"+this.parent;
	}
}
