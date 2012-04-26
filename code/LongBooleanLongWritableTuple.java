import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class LongBooleanLongWritableTuple implements Writable {
	Long l;
	Boolean b;
	Long parent;

	public void set(long l, boolean b, long parent){
		this.l = l;
		this.b = b;
		this.parent = parent;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		l = in.readLong();
		b = in.readBoolean();
		parent = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(l);
		out.writeBoolean(b);
		out.writeLong(parent);
	}
}
