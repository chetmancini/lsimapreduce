package lsimr.src.main.java.org.lsi.containers;
import java.io.DataOutputStream;
import java.io.IOException;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * @author chet
 *
 */
public class ForestFileOutputFormat extends FileOutputFormat<IntWritable, IntegerPair> {
	
	/**
	 * @author chet
	 *
	 * @param <K>
	 * @param <V>
	 */
	protected static class TreeRecordWriter<K, V> implements RecordWriter<K, V>{
		
		private static final String utf8 = "UTF-8";
		private DataOutputStream out;
		
		/**
		 * Constructor.
		 * @param out
		 * @throws IOException
		 */
		public TreeRecordWriter(DataOutputStream out) throws IOException{
			this.out = out;
		}
		
		/**
		 * Write Object
		 * @param o
		 * @throws IOException
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text){
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			}else{
				out.write(o.toString().getBytes(utf8));
			}
		}
		
		/**
		 * Write Key
		 * @param o
		 * @param closing
		 * @throws IOException
		 */
		private void writeKey(K key) throws IOException{
			Writable towrite = (Writable) key;
			towrite.write(this.out);
		}
		
		/**
		 * Write
		 */
		public synchronized void write(K key, V value) throws IOException {
			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			
			if(nullKey && nullValue){
				return;
			}
			writeKey(key);
			
			if(!nullValue){
				writeObject(value);
			}
		}
		
		/**
		 * Close the stream.
		 */
		public synchronized void close(Reporter reporter) throws IOException{
			out.close();
		}
	}
	

	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileout = fs.create(file, progress);
		return new TreeRecordWriter<K, V>(fileout);
	}

}
