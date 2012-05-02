package lsimr.src.main.java.org.lsi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Chet & Hugo
 */
public class LongBooleanWritableTuple implements WritableComparable<LongBooleanWritableTuple> {
	public Long l;
	public Boolean b;
	
	/**
	 * Constructor
	 */
	public LongBooleanWritableTuple(){
		this.l = new Long(0);
		this.b = new Boolean(false);
	}
	
	/**
	 * Constructor
	 * @param l
	 * @param b
	 */
	public LongBooleanWritableTuple(Long l, Boolean b){
		this.l = l;
		this.b = b;
	}
	
	/**
	 * Set
	 * @param l
	 * @param b
	 */
	public void set(long l, boolean b){
		this.l = l;
		this.b = b;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		l = in.readLong();
		b = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(l);
		out.writeBoolean(b);
	}

	@Override
	public int compareTo(LongBooleanWritableTuple o) {
		// TODO Auto-generated method stub
		if (this.l > o.l){
			return 1;
		}else if (this.l == o.l){
			return 0;
		}else{
			return -1;
		}
	}
	
	/**
	 * Equals
	 */
	public boolean equals(Object o){
		if(! (o instanceof LongBooleanWritableTuple)){
			return false;
		}
		LongBooleanWritableTuple other = (LongBooleanWritableTuple) o;
		return this.l == other.l;
	}
	
	/**
	 * Get hashcode.
	 */
	public int hashCode(){
		return l.hashCode();
	}
}
