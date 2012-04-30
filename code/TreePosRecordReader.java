import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;

/**
 * 
 */

/**
 * @author chet
 */
public class TreePosRecordReader extends RecordReader<K, V> {
	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	
	/**
	 * Constructor
	 * @param job
	 * @param split
	 * @throws IOException
	 */
	public TreePosRecordReader(JobConf job, FileSplit split) throws IOException{
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}
	
	/**
	 * Get next
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean next(Text key, Object value){
		if(!lineReader.next(lineKey, lineValue)){
			return false;
		}
		
		
		return true;
	}
	
	/**
	 * Create a new key.
	 */
	public Text createKey(){
		return new Text("");
	}
	
	/**
	 * Create a new value.
	 */
	public Object createValue(){
		return new Object();
	}
	
	/**
	 * Get position.
	 */
	public long getPos() throws IOException(){
		return lineReader.getPos();
	}
	
	/**
	 * Close the reader.
	 */
	public void close() throws IOException(){
		lineReader.close();
	}
	
	/**
	 * Get Progress
	 */
	public float getProgress(){
		return lineReader.getProgress();
	}
}
