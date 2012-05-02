package org.lsi.mapreduce;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.lsi.containers.BitMatrix;
import org.lsi.unionfind.UnionFind;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConnectedComponentsCounter extends Configured implements Tool {
    
	public static class MapFirstPass extends MapReduceBase implements
    Mapper<IntWritable, Text, IntWritable, IntBooleanWritableTuple> {
        
		private IntBooleanWritableTuple idAndValueCell = new IntBooleanWritableTuple();
		private IntWritable idColumn = new IntWritable();
        
		private final int defaultSizeInput = 1000;
		private int sizeInput;
		private int columnWidth;
		private URL url;
        
		public void configure(JobConf job) {
			sizeInput = job.getInt("connectedcomponentscounter.matrix.size",
                                   defaultSizeInput);
			columnWidth = job.getInt(
                                     "connectedcomponentscounter.matrix.columnWidth",
                                     (int) Math.sqrt(sizeInput));
			url = job.getResource("connectedcomponentscounter.matrix.inputurl");
		}
        
		// <get byte offset in input line, text of a line>
		// Return <idcolumn;<idcell,booleancell>>
		public void map(IntWritable key, Text value,
                        OutputCollector<IntWritable, IntBooleanWritableTuple> output,
                        Reporter reporter) throws IOException {
            
			if(key.get()%12!=0) 
				reporter.setStatus("Error modulo 12 in 1st pass map input is " + key.get()%12);
			
			int id = (int) Math.floor(key.get()/12);
			Float f = new Float(value.toString());
			
			idColumn.set(MrProj.getColumnGroupNbrsFromId(id, columnWidth, sizeInput)[0]);
			idAndValueCell.set(id, MrProj.getBoolean(f));
			output.collect(idColumn, idAndValueCell);
			
			if (MrProj.getColumnGroupNbrsFromId(id, columnWidth, sizeInput).length > 1) {
				idColumn.set(MrProj.getColumnGroupNbrsFromId(id, columnWidth, sizeInput)[1]);
				output.collect(idColumn, idAndValueCell);
			}
			
			
            BitMatrix m = MrProj.getMatrix(sizeInput, url);
            for(int i=0; i < sizeInput * sizeInput; i++){
            	
            	IntBooleanWritableTuple idcell = new IntBooleanWritableTuple();
            	
            	idcell.set(i, m.get_index(i, sizeInput));
            	idColumn.set(m.getColumnGroupNbrsFromId(i, columnWidth)[0]);
            	output.collect(idColumn, idcell);
            	
				if (m.getColumnGroupNbrsFromId(i, columnWidth).length > 1) {
					idColumn.set(m.getColumnGroupNbrsFromId(i, columnWidth)[1]);
					output.collect(idColumn, idcell);
				}
            }
            
		}
	}
    
	public static class ReduceFirstPass extends MapReduceBase
    implements
    Reducer<IntWritable, IntBooleanWritableTuple, IntBooleanWritableTuple, IntWritable> {
		// Get all the <id,boolean> of cells for one column
		// Return <<idcell,boolean>;id parent in this column>
		public void reduce(IntWritable idcolumn,
                           Iterator<IntBooleanWritableTuple> idsCells,
                           OutputCollector<IntBooleanWritableTuple, IntWritable> output,
                           Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idsCells);
            
			while (idsCells.hasNext()) {
				IntBooleanWritableTuple tuple = idsCells.next();
				output.collect(tuple,
                               new IntWritable(uf.getRoot(tuple.i)));
			}
		}
	}
    
	public static class MapSecondPass extends MapReduceBase
    implements
    Mapper<IntBooleanWritableTuple, IntWritable, Text, IntBooleanIntWritableTuple> {
        
		private IntBooleanIntWritableTuple idAndValueAndParentCell = new IntBooleanIntWritableTuple();
        
		private final int defaultSizeInput = 1000;
		private int sizeInput;
		private int columnWidth;
        
		public void configure(JobConf job) {
			sizeInput = job.getInt("connectedcomponentscounter.matrix.size",
                                   defaultSizeInput);
			columnWidth = job.getInt(
                                     "connectedcomponentscounter.matrix.columnWidth",
                                     (int) Math.sqrt(sizeInput));
		}
        
		// Input key is <id,boolean> and value is parentid
		// Return <someCommonKeyForAll;<idcell,booleancell,idparent>>
		public void map(IntBooleanWritableTuple key, IntWritable parent,
                        OutputCollector<Text, IntBooleanIntWritableTuple> output,
                        Reporter reporter) throws IOException {
			Text t = new Text("UniqueReducer");
            
			// TODO Plug correct function of Chet
			if (MrProj.isInBoundaryColumn(key.i, sizeInput, columnWidth)) {
				idAndValueAndParentCell.set(key.i, key.b, parent.get());
				output.collect(t, idAndValueAndParentCell);
			}
		}
	}
    
	public static class ReduceSecondPass extends MapReduceBase
    implements
    Reducer<Text, IntBooleanIntWritableTuple, IntBooleanWritableTuple, IntWritable> {
		IntBooleanWritableTuple outputKey = new IntBooleanWritableTuple();
        
		// Get all the <id,boolean,parent> of cells in boundary columns
		// Return <<idcell,boolean>;parentUpdated>
		public void reduce(
                           Text uselessKey,
                           Iterator<IntBooleanIntWritableTuple> idAndBooleanAndParentCells,
                           OutputCollector<IntBooleanWritableTuple, IntWritable> output,
                           Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idAndBooleanAndParentCells);
            
			while (idAndBooleanAndParentCells.hasNext()) {
				IntBooleanIntWritableTuple tuple = idAndBooleanAndParentCells
                .next();
				outputKey.set(tuple.l, tuple.b);
				output.collect(outputKey,
                               new IntWritable(uf.getMyMostSouthWestParent(tuple.l)));
			}
		}
	}
    
	public static class MapThirdPass extends MapReduceBase
    implements
    Mapper<IntBooleanWritableTuple, IntWritable, IntWritable, IntBooleanIntWritableTuple> {
        
		private IntWritable idColumn = new IntWritable();
		private IntBooleanIntWritableTuple idAndValueAndParentCell = new IntBooleanIntWritableTuple();
        
		private final int defaultSizeInput = 1000;
		private int sizeInput;
		private int columnWidth;
        
		public void configure(JobConf job) {
			sizeInput = job.getInt("connectedcomponentscounter.matrix.size",
                                   defaultSizeInput);
			columnWidth = job.getInt(
                                     "connectedcomponentscounter.matrix.columnWidth",
                                     (int) Math.sqrt(sizeInput));
		}
        
		// Input key is <id,boolean> and value is parentid
		// Return <idcolumn;<idcell,booleancell,idparent>>
		public void map(
                        IntBooleanWritableTuple key,
                        IntWritable parent,
                        OutputCollector<IntWritable, IntBooleanIntWritableTuple> output,
                        Reporter reporter) throws IOException {
			// TODO Plug the code of Chet correctly
			BitMatrix m = MrProj.getMyMatrix(sizeInput);
            
			for (int i = 0; i < sizeInput * sizeInput; i++) {
				idAndValueAndParentCell.set(key.l, key.b, parent.get());
				// Only add the left boundary column (avoid double counting)
				idColumn.set(m.getColumnNbrFromId(i, columnWidth)[0]);
				output.collect(idColumn, idAndValueAndParentCell);
			}
		}
	}
    
	public static class ReduceThirdPass extends MapReduceBase
    implements
    Reducer<IntWritable, IntBooleanIntWritableTuple, IntWritable, IntWritable> {
        
		IntWritable outputKey = new IntWritable();
		IntWritable outputValue = new IntWritable();
        
		// Get all the <id,boolean,parent> of cells in one column group
		// Return <parent,sizeSingleConnected>
		public void reduce(
                           IntWritable columnId,
                           Iterator<IntBooleanIntWritableTuple> idAndBooleanAndParentCells,
                           OutputCollector<IntWritable, IntWritable> output,
                           Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idAndBooleanAndParentCells);
            
			while (idAndBooleanAndParentCells.hasNext()) {
				IntBooleanIntWritableTuple tuple = idAndBooleanAndParentCells.next();
				outputKey.set(tuple.parent);
				outputValue.set(uf.getNbrSizeInThisColumn(tuple.parent));
				output.collect(outputKey, outputValue);
			}
		}
	}
    
	public JobConf createFirstPassConf(int matrixSize, int columnGroupWidth,
                                       String inputPath, String firstPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_firstPass");
        
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
        
		conf.setMapperClass(MapFirstPass.class);
		conf.setCombinerClass(ReduceFirstPass.class);
		conf.setReducerClass(ReduceFirstPass.class);
        
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
        
		if (matrixSize > 0)
			conf.setInt("connectedcomponentscounter.matrix.size", matrixSize);
        
		if (columnGroupWidth > 0)
			conf.setInt("connectedcomponentscounter.matrix.columnWidth",
                        columnGroupWidth);
        
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(firstPassOutputPath));
        
		return conf;
	}
    
	public JobConf createSecondPassConf(int matrixSize, int columnGroupWidth,
                                        String firstPassOutputPath, String secondPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_secondPass");
        
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
        
		conf.setMapperClass(MapSecondPass.class);
		conf.setCombinerClass(ReduceSecondPass.class);
		conf.setReducerClass(ReduceSecondPass.class);
        
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
        
		if (matrixSize > 0)
			conf.setInt("connectedcomponentscounter.matrix.size", matrixSize);
        
		if (columnGroupWidth > 0)
			conf.setInt("connectedcomponentscounter.matrix.columnWidth",
                        columnGroupWidth);
        
		FileInputFormat.setInputPaths(conf, firstPassOutputPath);
		FileOutputFormat.setOutputPath(conf, new Path(secondPassOutputPath));
        
		return conf;
	}
    
	public JobConf createThirdPassConf(int matrixSize, int columnGroupWidth,
                                       String firstPassOutputPath, String secondPassOutputPath,
                                       String outputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_secondPass");
        
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
        
		conf.setMapperClass(MapThirdPass.class);
		conf.setCombinerClass(ReduceThirdPass.class);
		conf.setReducerClass(ReduceThirdPass.class);
        
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
        
		if (matrixSize > 0)
			conf.setInt("connectedcomponentscounter.matrix.size", matrixSize);
        
		if (columnGroupWidth > 0)
			conf.setInt("connectedcomponentscounter.matrix.columnWidth",
                        columnGroupWidth);
        
		FileInputFormat.setInputPaths(conf, firstPassOutputPath + ","
                                      + secondPassOutputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
		return conf;
	}
    
	public int run(String[] args) throws Exception {
        
		int matrixSize = -1, columnGroupWidth = -1;
        
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-size".equals(args[i])) {
				matrixSize = new Integer(args[++i]);
			}
			if ("-columnWidth".equals(args[i])) {
				columnGroupWidth = new Integer(args[++i]);
			} else {
				other_args.add(args[i]);
			}
		}
        
		String inputPath = other_args.get(0);
		String firstPassOutputPath = inputPath + "/firstPass";
		String secondPassOutputPath = inputPath + "/secondPass";
		String outputPath = other_args.get(1);
        
		Job firstPass = new Job(createFirstPassConf(matrixSize,
                                                    columnGroupWidth, inputPath, firstPassOutputPath));
		Job secondPass = new Job(createSecondPassConf(matrixSize,
                                                      columnGroupWidth, firstPassOutputPath, secondPassOutputPath));
		Job thirdPass = new Job(createThirdPassConf(matrixSize,
                                                    columnGroupWidth, firstPassOutputPath, secondPassOutputPath,
                                                    outputPath));
        
		JobControl jc = new JobControl("Connected components counter");
		jc.addJob(firstPass);
		jc.addJob(secondPass);
		jc.addJob(thirdPass);
        
		// start the controller in a different thread, no worries as it does
		// that anyway
		Thread theController = new Thread(jc);
		theController.start();
        
		// poll until everything is done,
		// in the meantime justs output some status message
		while (!jc.allFinished()) {
			System.out.println("Jobs in waiting state: "
                               + jc.getWaitingJobs().size());
			System.out.println("Jobs in ready state: "
                               + jc.getReadyJobs().size());
			System.out.println("Jobs in running state: "
                               + jc.getRunningJobs().size());
			System.out.println("Jobs in success state: "
                               + jc.getSuccessfulJobs().size());
			System.out.println("Jobs in failed state: "
                               + jc.getFailedJobs().size());
			System.out.println("\n");
			// sleep 5 seconds
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
			}
		}
        
		// you have to check the status of each job submitted
		if (firstPass.getState() != Job.FAILED
            && firstPass.getState() != Job.DEPENDENT_FAILED
            && firstPass.getState() != Job.SUCCESS) {
			String states = "wordCountJob:  " + firstPass.getState() + "\n";
			throw new Exception(
                                "The state of wordCountJob is not in a complete state\n"
                                + states);
		}
		// now the second job
		if (secondPass.getState() != Job.FAILED
            && secondPass.getState() != Job.DEPENDENT_FAILED
            && secondPass.getState() != Job.SUCCESS) {
			String states = "job2Job:  " + secondPass.getState() + "\n";
			throw new Exception(
                                "The state of job2Job is not in a complete state\n"
                                + states);
		}
		// now the second job
		if (thirdPass.getState() != Job.FAILED
            && thirdPass.getState() != Job.DEPENDENT_FAILED
            && thirdPass.getState() != Job.SUCCESS) {
			String states = "job2Job:  " + thirdPass.getState() + "\n";
			throw new Exception(
                                "The state of job2Job is not in a complete state\n"
                                + states);
		}
        
		return 0;
	}
    
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
                                 new ConnectedComponentsCounter(), args);
		System.exit(res);
	}
    
}
