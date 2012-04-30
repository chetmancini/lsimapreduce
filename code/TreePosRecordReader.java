import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 
 */

/**
 * @author chet
 *
 */
public class TreePosRecordReader extends RecordReader<KEYIN, VALUEIN> {
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
	
	public Text createKey(){
		
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
