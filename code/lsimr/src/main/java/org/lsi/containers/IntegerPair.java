package org.lsi.containers;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Chet
 */
public class IntegerPair implements Writable{

	/**
	 * Can also represent "i"
	 */
	public Integer first;
	
	/**
	 * Can also represent "j"
	 */
	public Integer second;
	
	/**
	 * Constructor
	 */
	public IntegerPair(){
		
	}
	
	/**
	 * Constructor
	 * @param first
	 * @param second
	 */
	public IntegerPair(int first, int second){
		this.first = first;
		this.second = second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.first = in.readInt();
		this.second = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.first);
		out.writeInt(this.second);
	}
	
	/**
	 * Get first
	 * @return
	 */
	public int getFirst(){
		return this.first;
	}
	
	public int getI(){
		return this.getFirst();
	}
	
	/**
	 * Get second
	 * @return
	 */
	public int getSecond(){
		return this.second;
	}
	
	public int getJ(){
		return this.getSecond();
	}
	
	/**
	 * To string
	 */
	public String toString(){
		return first + "," + second;
	}
}
