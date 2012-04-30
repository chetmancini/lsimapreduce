import java.io.IOException;

import lsimr.src.main.java.org.lsi.containers.IntegerPair;

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
 * @param Key
 * @param Value
 */
public class TreePosRecordReader implements RecordReader<Text, IntegerPair> {
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
		this.lineReader = new LineRecordReader(job, split);
		lineKey = this.lineReader.createKey();
		lineValue = this.lineReader.createValue();
	}
	
	/**
	 * Get next
	 * format: "key,first,second"
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean next(Text key, IntegerPair value) throws IOException{
		if(!lineReader.next(lineKey, lineValue)){
			return false;
		}
		String[] pieces = lineValue.toString().split(",");
		if(pieces.length != 3){
			throw new IOException("Invalid record. not enough components.");
		}
		int first, second;
		try{
			first = Integer.parseInt(pieces[1].trim());
			second = Integer.parseInt(pieces[2].trim());
		}catch(NumberFormatException nfe){
			throw new IOException("Eror parsing integers in record.");
		}
		
		//not used. might try to make key an integer.
		int keyresult = Integer.parseInt(pieces[0].trim());
		
		key.set(pieces[0].trim());
		
		value.first = first;
		value.second = second;
		
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
	public IntegerPair createValue(){
		return new IntegerPair(0,0);
	}
	
	/**
	 * Get position.
	 */
	public long getPos() throws IOException{
		return lineReader.getPos();
	}
	
	/**
	 * Close the reader.
	 */
	public void close() throws IOException{
		lineReader.close();
	}
	
	/**
	 * Get Progress
	 */
	public float getProgress(){
		return lineReader.getProgress();
	}
}
