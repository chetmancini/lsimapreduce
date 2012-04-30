import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author chet
 */
public class IntegerPair implements Writable{

	public Integer first;
	public Integer second;
	
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
	
	/**
	 * Get second
	 * @return
	 */
	public int getSecond(){
		return this.second;
	}
}
