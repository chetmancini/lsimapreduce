import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class LongBooleanWritableTuple implements Writable {
	Long l;
	Boolean b;
	
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
}
