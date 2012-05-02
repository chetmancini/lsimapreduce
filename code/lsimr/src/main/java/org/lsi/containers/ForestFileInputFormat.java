package org.lsi.containers;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Chet
 */
public class ForestFileInputFormat extends FileInputFormat<IntWritable, IntegerPair> {
	public RecordReader<IntWritable, IntegerPair> getRecordReader(InputSplit input, JobConf job, Reporter reporter) throws IOException{
		reporter.setStatus(input.toString());
		return new TreePosRecordReader(job, (FileSplit) input);
	}
}
