import java.io.IOException;

import lsimr.src.main.java.org.lsi.containers.IntegerPair;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 */

/**
 * @author chet
 */
public class ForestFileInputFormat extends FileInputFormat<Text, IntegerPair> {
	public RecordReader<Text, IntegerPair> getRecordReader(InputSplit input, JobConf job, Reporter reporter) throws IOException{
		reporter.setStatus(input.toString());
		return new TreePosRecordReader(job, (FileSplit) input);
	}
}
