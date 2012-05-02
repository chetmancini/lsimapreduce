package org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntBooleanWritableTuple implements WritableComparable<IntBooleanWritableTuple>{
	public Integer i;
	public Boolean b;
	
	/**
	 * Constructor
	 */
	public IntBooleanWritableTuple(){
		this.i = new Integer(0);
		this.b = new Boolean(false);
	}
	
	/**
	 * Constructor
	 * @param l
	 * @param b
	 */
	public IntBooleanWritableTuple(Integer i, Boolean b){
		this.i = i;
		this.b = b;
	}
	
	/**
	 * Set
	 * @param i
	 * @param b
	 */
	public void set(int i, boolean b){
		this.i = i;
		this.b = b;
	}

	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		b = in.readBoolean();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeBoolean(b);
	}

	public int compareTo(IntBooleanWritableTuple o) {
		if (this.i > o.i){
			return 1;
		}else if (this.i == o.i){
			return 0;
		}else{
			return -1;
		}
	}
	
	/**
	 * Equals
	 */
	public boolean equals(Object o){
		if(! (o instanceof IntBooleanWritableTuple)){
			return false;
		}
		IntBooleanWritableTuple other = (IntBooleanWritableTuple) o;
		return this.i == other.i;
	}
	
	/**
	 * Get hashcode.
	 */
	public int hashCode(){
		return i.hashCode();
	}
}
