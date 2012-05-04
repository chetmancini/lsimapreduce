package org.lsi.mapreduce;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.lsi.containers.ComplexNumber;
import org.lsi.containers.KeyValue;
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

	/**
	 * MAP FIRST PASS
	 */
	public static class MapFirstPass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntIntWritableTuple> {

		private IntIntWritableTuple idAndParentCell = new IntIntWritableTuple();
		private IntWritable idColumn = new IntWritable();

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

		// <get byte offset in input line, text of a line>
		// Return <idcolumn;<idcell,booleancell>>
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {

			if (key.get() % 12 != 0)
				reporter.setStatus("Error modulo 12 in 1st pass map input is "
						+ key.get() % 12);

			Integer line = (int) Math.floor(key.get() / 12);
			Float f = new Float(value.toString());
			/**
			 * Only put in the iterator if there is a vertex.
			 */
			if (MrProj.getBoolean(f)) {
				/**
				 * Loop over all possible column groups (could be two of them
				 * for a boundary column.
				 */
				for (Integer i : MrProj.getColumnGroupNbrsFromLine(line,
						columnWidth, sizeInput)) {
					int idInColumnGroup = MrProj.getIdInColumnGroupFromLine(line, i, columnWidth, sizeInput);
					idAndParentCell.set(idInColumnGroup, idInColumnGroup);
					idColumn.set(i);
					output.collect(idColumn, idAndParentCell);
				}
				
			}
		}
	}

	/**
	 * REDUCE FIRST PASS
	 */
	public static class ReduceFirstPass extends MapReduceBase implements
			Reducer<IntWritable, IntIntWritableTuple, IntWritable, IntIntWritableTuple> {

        private IntIntWritableTuple root = new IntIntWritableTuple(); 
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

		// Get all the <id,boolean> of cells for one column
		// Return <<idcell,boolean>;id parent in this column>
		public void reduce(IntWritable idcolumn,
				Iterator<IntIntWritableTuple> idsCells,
				OutputCollector<IntWritable, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {
			UnionFind uf = new UnionFind(idcolumn, idsCells, sizeInput, columnWidth);
			HashMap<ComplexNumber, ComplexNumber> roots = uf.getRoots();
			
			for(ComplexNumber complexCell : roots.keySet()) {
				ComplexNumber complexRoot = roots.get(complexCell);
                root.set(complexCell.index, complexRoot == null ? -42 : complexRoot.index);

                if(complexCell.groupid != idcolumn.get())
                	output.collect(new IntWritable(-42), new IntIntWritableTuple(idcolumn.get(),complexCell.groupid));
                else
                	output.collect(new IntWritable(complexCell.groupid), root);
			}
		}
	}

	
	/**
	 * MAP SECOND PASS
	 */
	public static class MapSecondPass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntIntIntWritableTuple> {

		private IntIntIntWritableTuple columnGpNbrAndIdCellAndParentInColumn = new IntIntIntWritableTuple();

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

		// Input is <byteoffset,line>
		// Return <someCommonKeyForAll;<idcolumnGp,localidcell,localidparent>>
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntIntIntWritableTuple> output,
				Reporter reporter) throws IOException {
			Text t = new Text("UniqueReducer");
			
			KeyValue<IntWritable, IntIntWritableTuple> kv = MrProj.parseLineSecondMapper(value);
			
			if (MrProj.isInBoundaryColumnLocal(kv.getValue().i, columnWidth, sizeInput)) {
				columnGpNbrAndIdCellAndParentInColumn.set(kv.getKey().get(), kv.getValue().i, kv.getValue().parent);
				output.collect(t, columnGpNbrAndIdCellAndParentInColumn);
			}
		}
	}

	/**
	 * REDUCE SECOND PASS
	 */
	public static class ReduceSecondPass extends MapReduceBase implements
			Reducer<Text, IntIntIntWritableTuple, IntWritable, IntIntWritableTuple> {
		IntIntWritableTuple root = new IntIntWritableTuple();
		
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

		// Get all the <idcolumnGp,localIdCell,localIdParent> of cells in boundary columns
		// Return <idColumn,<localCellId,localParentId>>
		public void reduce(Text uselessKey,
				Iterator<IntIntIntWritableTuple> columnAndLocalIdCellAndParent,
				OutputCollector<IntWritable, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {
			UnionFind uf = new UnionFind(columnAndLocalIdCellAndParent, sizeInput, columnWidth);
			HashMap<ComplexNumber, ComplexNumber> roots = uf.getRoots();

			for(ComplexNumber complexCell : roots.keySet()) {
				//Not output right boundary column except if last column is the end of a column group
				if(complexCell.index<sizeInput || complexCell.groupid == (int)Math.ceil((float)sizeInput/(columnWidth-1))- 1) {
					ComplexNumber complexRoot = roots.get(complexCell);
					root.set(complexCell.index, complexRoot == null ? -42
							: complexRoot.index);
					output.collect(new IntWritable(complexCell.groupid), root);
				}
			}
		}
	}

	/**
	 * MAP THIRD PASS
	 */
	public static class MapThirdPass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntIntWritableTuple> {
	
		private IntIntWritableTuple columnGpNbrAndIdCellAndParentInColumn = new IntIntWritableTuple();

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



		// Input is <byteoffset,line>
		// Return <idColumngp,<localidcell,localidparent>>
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {			
			KeyValue<IntWritable, IntIntWritableTuple> kv = MrProj.parseLineSecondMapper(value);
		    if(kv.getValue().i < (columnWidth -1) * sizeInput || kv.getKey().get()==(int)Math.ceil((float)sizeInput/(columnWidth-1)))
			columnGpNbrAndIdCellAndParentInColumn.set(kv.getValue().i, kv.getValue().parent);
			output.collect( kv.getKey(), columnGpNbrAndIdCellAndParentInColumn);
		}
	}

	/**
	 * REDUCE THIRD PASS
	 */
	public static class ReduceThirdPass extends MapReduceBase implements
			Reducer<IntWritable, IntIntWritableTuple, IntWritable, IntWritable> {

		IntWritable parentGlobalId = new IntWritable();
		IntWritable sizeComponent = new IntWritable();

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

		// Get all the <localidcell,localidparent> of cells in one column group
		// Return <parent,sizeSingleConnected>
		public void reduce(IntWritable columnGroupId,
				Iterator<IntIntWritableTuple> idAndParentCells,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			UnionFind uf = new UnionFind(columnGroupId, idAndParentCells, sizeInput, columnWidth);
//			HashMap<ComplexNumber, ComplexNumber> roots = uf.getRoots();
//			
//			for(ComplexNumber complexRoot : roots.values()) {
//				parentGlobalId.set(MrProj.getGlobalFromIdInColumnGroup(complexRoot.index, complexRoot.groupid, columnWidth, sizeInput));
//				//TODO get the real size of the component
//				sizeComponent.set(complexRoot.groupid);
//				output.collect(parentGlobalId,sizeComponent);
//			}
			
			HashMap<ComplexNumber, Integer> sizesForRoot = uf.getSizes();
			
			for(ComplexNumber c : sizesForRoot.keySet()) {
				parentGlobalId.set(MrProj.getGlobalFromIdInColumnGroup(c.index, c.groupid, columnWidth, sizeInput));
//				sizeComponent.set(sizesForRoot.get(c));
				sizeComponent.set(c.groupid);
				output.collect(parentGlobalId,sizeComponent);
			}
		}
	}
	
	/**
	 * MAP FOURTH PASS
	 */
	public static class MapFourthPass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		// Input is <byteoffset,line>
		// Return <globalIdRoot,sizeConnectedComponent>
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {			
			KeyValue<IntWritable, IntWritable> kv = MrProj.parseLineFourthMapper(value);
			output.collect( kv.getKey(), kv.getValue());
		}
	}
	
	/**
	 * REDUCE FOURTH PASS
	 */
	public static class ReduceFourthPass extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		// Input is iterator <sizeConnectedComponent> for each globabIdRoot
		// Return <globalIdRoot,sum of sizeConnectedComponent>
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {		
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}


	public JobConf createFirstPassConf(int matrixSize, int columnGroupWidth,
			String inputPath, String firstPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_firstPass");

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(IntIntWritableTuple.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapFirstPass.class);
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

		conf.setMapOutputKeyClass(Text.class); 
		conf.setMapOutputValueClass(IntIntIntWritableTuple.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntIntWritableTuple.class);

		conf.setMapperClass(MapSecondPass.class);
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
			String thirdPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_thirdPass");

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(IntIntWritableTuple.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapThirdPass.class);
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
		FileOutputFormat.setOutputPath(conf, new Path(thirdPassOutputPath));

		return conf;
	}
	
	public JobConf createFourthPassConf(int matrixSize, int columnGroupWidth, String thirdPassOutputPath,
			String outputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_fourthPass");

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapFourthPass.class);
		conf.setReducerClass(ReduceFourthPass.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (matrixSize > 0)
			conf.setInt("connectedcomponentscounter.matrix.size", matrixSize);

		if (columnGroupWidth > 0)
			conf.setInt("connectedcomponentscounter.matrix.columnWidth",
					columnGroupWidth);

		FileInputFormat.setInputPaths(conf, thirdPassOutputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {

		int matrixSize = -1, columnGroupWidth = -1;

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("size".equals(args[i])) {
				matrixSize = new Integer(args[++i]);
			}
			if ("columnWidth".equals(args[i])) {
				columnGroupWidth = new Integer(args[++i]);
			} else {
				other_args.add(args[i]);
			}
		}

		String inputPath = other_args.get(0);
		String firstPassOutputPath = inputPath + "/firstPass";
		String secondPassOutputPath = inputPath + "/secondPass";
		String thirdPassOutputPath = inputPath + "/thirdPass";
		String outputPath = other_args.get(1);

		Job firstPass = new Job(createFirstPassConf(matrixSize,
				columnGroupWidth, inputPath, firstPassOutputPath));
		Job secondPass = new Job(createSecondPassConf(matrixSize,
				columnGroupWidth, firstPassOutputPath, secondPassOutputPath));
		secondPass.addDependingJob(firstPass);
		Job thirdPass = new Job(createThirdPassConf(matrixSize,
				columnGroupWidth, firstPassOutputPath, secondPassOutputPath,
				thirdPassOutputPath));
		thirdPass.addDependingJob(secondPass);
		Job fourthPass = new Job(createFourthPassConf(matrixSize,
				columnGroupWidth, thirdPassOutputPath,
				outputPath));
		fourthPass.addDependingJob(thirdPass);

		JobControl jc = new JobControl("Connected components counter");
		jc.addJob(firstPass);
		jc.addJob(secondPass);
		jc.addJob(thirdPass);
		jc.addJob(fourthPass);

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
		// now the fourth job
		if (fourthPass.getState() != Job.FAILED
				&& fourthPass.getState() != Job.DEPENDENT_FAILED
				&& fourthPass.getState() != Job.SUCCESS) {
			String states = "fourthPassJob:  " + fourthPass.getState() + "\n";
			throw new Exception(
					"The state of fourthPassJob is not in a complete state\n"
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
