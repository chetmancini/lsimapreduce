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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
			Mapper<LongWritable, Text, IntWritable, IntIntWritableTuple> {

		private IntIntWritableTuple idAndParentCell = new IntIntWritableTuple();
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
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {

			if (key.get() % 12 != 0)
				reporter.setStatus("Error modulo 12 in 1st pass map input is "
						+ key.get() % 12);

			Integer id = (int) Math.floor(key.get() / 12);
			Float f = new Float(value.toString());

			/**
			 * Only put in the iterator if there is a vertex.
			 */
			if (MrProj.getBoolean(f)) {
				/**
				 * initialize the root to -1 to indicate we haven't scanned
				 * neighbors yet.
				 */
				idAndParentCell.set(id, -1);

				/**
				 * Loop over all possible column groups (could be two of them
				 * for a boundary column.
				 */
				for (Integer i : MrProj.getColumnGroupNbrsFromId(id,
						columnWidth, sizeInput)) {
					idColumn.set(i);
					output.collect(idColumn, idAndParentCell);
				}
		
				// BitMatrix m = MrProj.getMatrix(sizeInput, url);
				// for(int i=0; i < sizeInput * sizeInput; i++){
				//
				// IntWritable idcell = new IntWritable();
				//
				// idcell.set(i, m.get_index(i, sizeInput));
				// idColumn.set(m.getColumnGroupNbrsFromId(i, columnWidth)[0]);
				// output.collect(idColumn, idcell);
				//
				// if (m.getColumnGroupNbrsFromId(i, columnWidth).length > 1) {
				// idColumn.set(m.getColumnGroupNbrsFromId(i, columnWidth)[1]);
				// output.collect(idColumn, idcell);
				// }
				// }

			}
		}
	}

	public static class ReduceFirstPass extends MapReduceBase implements
			Reducer<IntWritable, IntIntWritableTuple, IntWritable, IntWritable> {

		IntWritable cellId = new IntWritable();
		IntWritable parentId = new IntWritable();

		// Get all the <id,boolean> of cells for one column
		// Return <<idcell,boolean>;id parent in this column>
		public void reduce(IntWritable idcolumn,
				Iterator<IntIntWritableTuple> idsCells,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idsCells);

			while (idsCells.hasNext()) {
				IntIntWritableTuple cellAndParentIds = idsCells.next();

				cellId.set(cellAndParentIds.i);
				parentId.set(uf.getRoot(cellAndParentIds.i));

				output.collect(cellId, parentId);
			}
		}
	}

	public static class MapSecondPass extends MapReduceBase implements
			Mapper<IntWritable, IntWritable, Text, IntIntWritableTuple> {

		private IntIntWritableTuple idAndValueAndParentCell = new IntIntWritableTuple();

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
		public void map(IntWritable cellId, IntWritable parentId,
				OutputCollector<Text, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {
			Text t = new Text("UniqueReducer");

			// TODO Plug correct function of Chet
			if (MrProj.isInBoundaryColumn(cellId.get(), sizeInput, columnWidth)) {
				idAndValueAndParentCell.set(cellId.get(), parentId.get());
				output.collect(t, idAndValueAndParentCell);
			}
		}
	}

	public static class ReduceSecondPass extends MapReduceBase implements
			Reducer<Text, IntIntWritableTuple, IntWritable, IntWritable> {
		IntWritable cellId = new IntWritable();
		IntWritable parentId = new IntWritable();

		// Get all the <id,boolean,parent> of cells in boundary columns
		// Return <<idcell,boolean>;parentUpdated>
		public void reduce(Text uselessKey,
				Iterator<IntIntWritableTuple> idsCells,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idsCells);

			while (idsCells.hasNext()) {
				IntIntWritableTuple cellAndParentIds = idsCells.next();

				cellId.set(cellAndParentIds.i);
				parentId.set(uf.getRoot(cellAndParentIds.i));

				if (parentId.get() == -1)
					reporter.setStatus("ERROR: Parent for cell " + cellId.get()
							+ " has not been computed");
			}
		}
	}

	public static class MapThirdPass extends MapReduceBase implements
			Mapper<IntWritable, IntWritable, IntWritable, IntIntWritableTuple> {

		private IntWritable idColumn = new IntWritable();
		private IntIntWritableTuple idAndParentCell = new IntIntWritableTuple();

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

		// Input key is idCell and value is idParent
		// Return <idcolumn;<idcell,idParent>>
		public void map(IntWritable key, IntWritable parent,
				OutputCollector<IntWritable, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {

			idAndParentCell.set(key.get(), parent.get());

			/**
			 * Only send boundary column in one reducer
			 */
			idColumn.set(MrProj.getColumngroupNbrsFromId(key, columnWidth,
					sizeInput)[0]);
			output.collect(idColumn, idAndParentCell);
		}
	}

	public static class ReduceThirdPass extends MapReduceBase implements
			Reducer<IntWritable, IntIntWritableTuple, IntWritable, IntWritable> {

		IntWritable outputKey = new IntWritable();
		IntWritable outputValue = new IntWritable();

		// Get all the <id,boolean,parent> of cells in one column group
		// Return <parent,sizeSingleConnected>
		public void reduce(IntWritable columnId,
				Iterator<IntIntWritableTuple> idAndParentCells,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idAndParentCells);

			while (idAndParentCells.hasNext()) {
				IntIntWritableTuple tuple = idAndParentCells.next();
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
//		conf.setCombinerClass(ReduceFirstPass.class);
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
//		conf.setCombinerClass(ReduceSecondPass.class);
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
//		conf.setCombinerClass(ReduceThirdPass.class);
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

	@Override
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
			String states = "firstPassJob:  " + firstPass.getState() + "\n";
			throw new Exception(
					"The state of firstPassJob is not in a complete state\n"
							+ states);
		}
		// now the second job
		if (secondPass.getState() != Job.FAILED
				&& secondPass.getState() != Job.DEPENDENT_FAILED
				&& secondPass.getState() != Job.SUCCESS) {
			String states = "secondPassJob:  " + secondPass.getState() + "\n";
			throw new Exception(
					"The state of secondPassJob is not in a complete state\n"
							+ states);
		}
		// now the third job
		if (thirdPass.getState() != Job.FAILED
				&& thirdPass.getState() != Job.DEPENDENT_FAILED
				&& thirdPass.getState() != Job.SUCCESS) {
			String states = "thirdPassJob:  " + thirdPass.getState() + "\n";
			throw new Exception(
					"The state of thirdPassJob is not in a complete state\n"
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
