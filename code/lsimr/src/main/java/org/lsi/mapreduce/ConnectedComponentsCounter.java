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
import org.lsi.containers.FullGraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConnectedComponentsCounter extends Configured implements Tool {

	static enum STATS {
		VERTICES, EDGES, COMPONENTS
	}
	
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
			MrProj.setVariables(job.getFloat(
					"connectedcomponentscounter.matrix.defaultDensity", 
					0.59f));
			
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
				reporter.getCounter(STATS.VERTICES).increment(1);
				
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
			Reducer<IntWritable, IntIntWritableTuple, IntIntWritableTuple, IntIntWritableTuple> {
		
        private IntIntWritableTuple cell = new IntIntWritableTuple(); 
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
			MrProj.setVariables(job.getFloat(
					"connectedcomponentscounter.matrix.defaultDensity", 
					0.59f));
        }

		// Get all the <localIdCell,localIdParent> of cells for one column
		// Return <<idColumnGp,localCellId>,<idColumGp,localParentId>>
		public void reduce(IntWritable idcolumn,
				Iterator<IntIntWritableTuple> idsCells,
				OutputCollector<IntIntWritableTuple, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {
			UnionFind uf = new UnionFind(idcolumn, idsCells, sizeInput, columnWidth);
			
			reporter.getCounter(STATS.EDGES).increment(uf.getEdges());
			HashMap<ComplexNumber, ComplexNumber> roots = uf.getRoots();
			
			for(ComplexNumber complexCell : roots.keySet()) {
				ComplexNumber complexRoot = roots.get(complexCell);
				cell.set(complexCell.groupid, complexCell.index);
                root.set(complexRoot.groupid, complexRoot.index);
                output.collect(cell, root);
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
			MrProj.setVariables(job.getFloat(
					"connectedcomponentscounter.matrix.defaultDensity", 
					0.59f));
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
			Reducer<Text, IntIntIntWritableTuple, IntIntWritableTuple, IntIntWritableTuple> {
		IntIntWritableTuple cell = new IntIntWritableTuple();
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
			MrProj.setVariables(job.getFloat(
					"connectedcomponentscounter.matrix.defaultDensity", 
					0.59f));
        }

		// Get all the <idcolumnGp,localIdCell,localIdParent> of cells in boundary columns
		// Return <idColumn,<localCellId,globalParentId>>
		public void reduce(Text uselessKey,
				Iterator<IntIntIntWritableTuple> columnAndLocalIdCellAndParent,
				OutputCollector<IntIntWritableTuple, IntIntWritableTuple> output,
				Reporter reporter) throws IOException {
			UnionFind uf = new UnionFind(columnAndLocalIdCellAndParent, sizeInput, columnWidth);
			HashMap<ComplexNumber, ComplexNumber> roots = uf.getRoots();

			for(ComplexNumber complexCell : roots.keySet()) {
				//Not output right boundary column except if last column is the end of a column group
				if(complexCell.index<sizeInput || complexCell.groupid == (int)Math.ceil((float)sizeInput/(columnWidth-1))- 1) {
					ComplexNumber complexRoot = roots.get(complexCell);
					cell.set(complexCell.groupid, complexCell.index);
					root.set(complexRoot.groupid, complexRoot.index);
					output.collect(cell, root);
				}
			}
		}
	}

	/**
	 * MAP THIRD PASS
	 */
	public static class MapThirdPass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntIntIntIntWritableTuple> {

		IntWritable idColumn = new IntWritable();
		IntIntIntIntWritableTuple cellAndRoot = new IntIntIntIntWritableTuple();
		
        private final int defaultSizeInput = 1000;
		private int sizeInput;
		private int columnWidth;

		public void configure(JobConf job) {
			sizeInput = job.getInt("connectedcomponentscounter.matrix.size",
					defaultSizeInput);
			columnWidth = job.getInt(
					"connectedcomponentscounter.matrix.columnWidth",
					(int) Math.sqrt(sizeInput));
			MrProj.setVariables(job.getFloat(
					"connectedcomponentscounter.matrix.defaultDensity", 
					0.59f));
        }



		// Input is <byteoffset,line>
		// Return <<idColumngp,localidcell>,<idColumngp,localidparent>>
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntIntIntIntWritableTuple> output,
				Reporter reporter) throws IOException {			
			KeyValue<IntIntWritableTuple, IntIntWritableTuple> kv = MrProj.parseLineThirdMapper(value);
		    if(kv.getKey().parent < (columnWidth -1) * sizeInput || kv.getKey().i==(int)Math.ceil((float)sizeInput/(columnWidth-1)-1)){
		    	idColumn.set(kv.getKey().i);
		    	cellAndRoot.set(kv.getKey().i,kv.getKey().parent,kv.getValue().i,kv.getValue().parent);
		    	output.collect(idColumn, cellAndRoot);
		    }
		}
	}

	/**
	 * REDUCE THIRD PASS
	 */
	public static class ReduceThirdPass extends MapReduceBase implements
			Reducer<IntWritable, IntIntIntIntWritableTuple, IntWritable, IntWritable> {

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
			MrProj.setVariables(job.getFloat(
					"connectedcomponentscounter.matrix.defaultDensity", 
					0.59f));
		}

		// Get all the <localidcell,localidparent> of cells in one column group
		// Return <parent,sizeSingleConnected>
		public void reduce(IntWritable columnGroupId,
				Iterator<IntIntIntIntWritableTuple> cellAndRoot,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
            HashMap<ComplexNumber, ComplexNumber> vec = new HashMap<ComplexNumber,ComplexNumber>();
			while(cellAndRoot.hasNext()){
                IntIntIntIntWritableTuple cr = cellAndRoot.next();
                ComplexNumber cn = new ComplexNumber(cr.groupidi, cr.i);
                ComplexNumber cp = new ComplexNumber(cr.groupidp, cr.parent);
                if(!vec.containsKey(cn) || vec.get(cn).lessThan(cp))
                    vec.put(cn,cp);
            }
            FullGraph fg = new FullGraph();
            fg.vertices=vec;
            fg.m=sizeInput;
            fg.n=columnWidth;            
                
            UnionFind uf = new UnionFind(fg);

			HashMap<ComplexNumber, Integer> sizesForRoot = uf.getSizes();
			
			for(ComplexNumber c : sizesForRoot.keySet()) {
				parentGlobalId.set(MrProj.getGlobalFromIdInColumnGroup(c.index, c.groupid, columnWidth, sizeInput));
				sizeComponent.set(sizesForRoot.get(c));
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
			reporter.getCounter(STATS.COMPONENTS).increment(1);
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}


	public JobConf createFirstPassConf(int matrixSize, int columnGroupWidth,
			float defaultDensity, String inputPath, String firstPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_firstPass");

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(IntIntWritableTuple.class);
		
		conf.setOutputKeyClass(IntIntWritableTuple.class);
		conf.setOutputValueClass(IntIntWritableTuple.class);

		conf.setMapperClass(MapFirstPass.class);
		conf.setReducerClass(ReduceFirstPass.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (matrixSize > 0)
			conf.setInt("connectedcomponentscounter.matrix.size", matrixSize);

		if (columnGroupWidth > 0)
			conf.setInt("connectedcomponentscounter.matrix.columnWidth",
					columnGroupWidth);
		if (defaultDensity != 0.59f){
			conf.setFloat("connectedcomponentscounter.matrix.defaultDensity", 
					defaultDensity);
		}

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(firstPassOutputPath));

		return conf;
	}

	public JobConf createSecondPassConf(int matrixSize, int columnGroupWidth,
			float defaultDensity, String firstPassOutputPath, String secondPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_secondPass");

		conf.setMapOutputKeyClass(Text.class); 
		conf.setMapOutputValueClass(IntIntIntWritableTuple.class);
		
		conf.setOutputKeyClass(IntIntWritableTuple.class);
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
		if (defaultDensity != 0.59f){
			conf.setFloat("connectedcomponentscounter.matrix.defaultDensity", 
					defaultDensity);
		}

		FileInputFormat.setInputPaths(conf, firstPassOutputPath);
		FileOutputFormat.setOutputPath(conf, new Path(secondPassOutputPath));

		return conf;
	}

	public JobConf createThirdPassConf(int matrixSize, int columnGroupWidth,
			float defaultDensity, String firstPassOutputPath, String secondPassOutputPath,
			String thirdPassOutputPath) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_thirdPass");

		conf.setMapOutputKeyClass(IntWritable.class); 
		conf.setMapOutputValueClass(IntIntIntIntWritableTuple.class);
		
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
		
		if (defaultDensity != 0.59f){
			conf.setFloat("connectedcomponentscounter.matrix.defaultDensity", 
					defaultDensity);
		}

		FileInputFormat.setInputPaths(conf, firstPassOutputPath + ","
				+ secondPassOutputPath);
		FileOutputFormat.setOutputPath(conf, new Path(thirdPassOutputPath));

		return conf;
	}
	
	public JobConf createFourthPassConf(int matrixSize, int columnGroupWidth, 
			float defaultDensity, String thirdPassOutputPath, String outputPath) {
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
		if (defaultDensity != 0.59f){
			conf.setFloat("connectedcomponentscounter.matrix.defaultDensity", 
					defaultDensity);
		}

		FileInputFormat.setInputPaths(conf, thirdPassOutputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {

		int matrixSize = -1, columnGroupWidth = -1;
		float defaultDensity = 0.59f;

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("size".equals(args[i])) {
				matrixSize = new Integer(args[++i]);
			}
			if ("columnWidth".equals(args[i])) {
				columnGroupWidth = new Integer(args[++i]);
			}
			if ("defaultDensity".equals(args[i])){
				defaultDensity = new Float(args[++i]);
			}
			else{
				other_args.add(args[i]);
			}
		}

		String inputPath = other_args.get(0);
		String firstPassOutputPath = inputPath + "/firstPass";
		String secondPassOutputPath = inputPath + "/secondPass";
		String thirdPassOutputPath = inputPath + "/thirdPass";
		String outputPath = other_args.get(1);
		
		System.out.println("inputPath :"+inputPath);
		System.out.println("outputPath :"+outputPath);
		
		RunningJob firstPassRunning = JobClient.runJob(createFirstPassConf(matrixSize,
				columnGroupWidth, defaultDensity, inputPath, firstPassOutputPath));
		
		while(!firstPassRunning.isComplete()) {
			System.out.println("First pass running");
			Thread.sleep(2000);
		}
		
		if(!firstPassRunning.isSuccessful()) {
			System.out.println("First pass failed");
			return -1;
		}
				
		RunningJob secondPassRunning = JobClient.runJob(createSecondPassConf(matrixSize,
				columnGroupWidth, defaultDensity, firstPassOutputPath, secondPassOutputPath));
		
		while(!secondPassRunning.isComplete()) {
			System.out.println("Second pass running");
			Thread.sleep(2000);
		}
		
		if(!secondPassRunning.isSuccessful()) {
			System.out.println("Second pass failed");
			return -1;
		}
		
		RunningJob thirdPassRunning = JobClient.runJob(createThirdPassConf(matrixSize,
				columnGroupWidth, defaultDensity, firstPassOutputPath, secondPassOutputPath, thirdPassOutputPath));
		
		while(!thirdPassRunning.isComplete()) {
			System.out.println("Third pass running");
			Thread.sleep(2000);
		}

		if(!thirdPassRunning.isSuccessful()) {
			System.out.println("Third pass failed");
			return -1;
		}
		
		RunningJob fourthPassRunning = JobClient.runJob(createFourthPassConf(matrixSize,
				columnGroupWidth, defaultDensity, thirdPassOutputPath, outputPath));
		
		while(!fourthPassRunning.isComplete()) {
			System.out.println("Fourth pass running");
			Thread.sleep(2000);
		}

		if(!fourthPassRunning.isSuccessful()) {
			System.out.println("Fourth pass failed");
			return -1;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new ConnectedComponentsCounter(), args);
		System.exit(res);
	}

}
