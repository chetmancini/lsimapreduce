import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
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
			Mapper<LongWritable, Text, LongWritable, LongBooleanWritableTuple> {

		private LongBooleanWritableTuple idCase = new LongBooleanWritableTuple();
		private LongWritable idColumn = new LongWritable();

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

		// Value is the whole txt file ?
		// Return <idcolumn;<idcase,booleancase>>
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, LongBooleanWritableTuple> output,
				Reporter reporter) throws IOException {

			String line = value.toString();
			// TODO Plug the code of Chet correctly
			MyMatrix m = MRProj.getMyMatrix(sizeInput);

			for (long i = 0; i < sizeInput * sizeInput; i++) {
				idCase.set(i, m.get(i));
				idColumn.set(m.getColumnNbrFromId(i, columnWidth)[0]);
				output.collect(idColumn, idCase);
				// Case of the boundary column belonging to 2 columns
				if (m.getColumnNbrFromId(i, columnWidth).length > 1) {
					idColumn.set(m.getColumnNbrFromId(i, columnWidth)[1]);
					output.collect(idColumn, idCase);
				}
			}
		}
	}

	public static class ReduceFirstPass extends MapReduceBase
			implements
			Reducer<LongWritable, LongBooleanWritableTuple, LongBooleanWritableTuple, LongWritable> {
		// Get all the <id,boolean> of cases for one column
		// Return <<idcase,boolean>;id parent in this column>
		public void reduce(LongWritable idcolumn,
				Iterator<LongBooleanWritableTuple> idsCases,
				OutputCollector<LongBooleanWritableTuple, LongWritable> output,
				Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idsCases);

			while (idsCases.hasNext()) {
				LongBooleanWritableTuple tuple = idsCases.next();
				output.collect(tuple,
						new LongWritable(uf.getMyMostSouthWestParent(tuple.l)));
			}
		}
	}

	public static class MapSecondPass extends MapReduceBase
			implements
			Mapper<LongBooleanWritableTuple, LongWritable, Text, LongBooleanLongWritableTuple> {

		private LongBooleanLongWritableTuple idAndValueAndParentCase = new LongBooleanLongWritableTuple();

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
		// Return <someCommonKeyForAll;<idcase,booleancase,idparent>>
		public void map(LongBooleanWritableTuple key, LongWritable parent,
				OutputCollector<Text, LongBooleanLongWritableTuple> output,
				Reporter reporter) throws IOException {
			Text t = new Text("UniqueReducer");

			// TODO Plug correct function of Chet
			if (MrProj.isInBoundaryColumn(key.l, sizeInput, columnWidth)) {
				idAndValueAndParentCase.set(key.l, key.b, parent.get());
				output.collect(t, idAndValueAndParentCase);
			}
		}
	}

	public static class ReduceSecondPass extends MapReduceBase
			implements
			Reducer<Text, LongBooleanLongWritableTuple, LongBooleanWritableTuple, LongWritable> {
		LongBooleanWritableTuple outputKey = new LongBooleanWritableTuple();

		// Get all the <id,boolean,parent> of cases in boundary columns
		// Return <<idcase,boolean>;parentUpdated>
		public void reduce(
				Text uselessKey,
				Iterator<LongBooleanLongWritableTuple> idAndBooleanAndParentCases,
				OutputCollector<LongBooleanWritableTuple, LongWritable> output,
				Reporter reporter) throws IOException {
			// TODO Plug the code of Sean Correctly
			UnionFind uf = new UnionFind(idAndBooleanAndParentCases);

			while (idAndBooleanAndParentCases.hasNext()) {
				LongBooleanLongWritableTuple tuple = idAndBooleanAndParentCases
						.next();
				outputKey.set(tuple.l, tuple.b);
				output.collect(outputKey,
						new LongWritable(uf.getMyMostSouthWestParent(tuple.l)));
			}
		}
	}

	public static class MapThirdPass extends MapReduceBase
			implements
			Mapper<LongBooleanWritableTuple, LongWritable, Text, LongBooleanLongWritableTuple> {

		//TODO
//		private LongBooleanLongWritableTuple idAndValueAndParentCase = new LongBooleanLongWritableTuple();
//
//		private final int defaultSizeInput = 1000;
//		private int sizeInput;
//		private int columnWidth;
//
//		public void configure(JobConf job) {
//			sizeInput = job.getInt("connectedcomponentscounter.matrix.size",
//					defaultSizeInput);
//			columnWidth = job.getInt(
//					"connectedcomponentscounter.matrix.columnWidth",
//					(int) Math.sqrt(sizeInput));
//		}

		// Input key is <id,boolean> and value is parentid
		// Return <someCommonKeyForAll;<idcase,booleancase,idparent>>
		public void map(LongBooleanWritableTuple key, LongWritable parent,
				OutputCollector<Text, LongBooleanLongWritableTuple> output,
				Reporter reporter) throws IOException {
//			Text t = new Text("UniqueReducer");
//
//			// TODO Plug correct function of Chet
//			if (MrProj.isInBoundaryColumn(key.l, sizeInput, columnWidth)) {
//				idAndValueAndParentCase.set(key.l, key.b, parent.get());
//				output.collect(t, idAndValueAndParentCase);
//			}
		}
	}

	public static class ReduceThirdPass extends MapReduceBase
			implements
			Reducer<Text, LongBooleanLongWritableTuple, LongBooleanWritableTuple, LongWritable> {
		
		//TODO 
		
//		LongBooleanWritableTuple outputKey = new LongBooleanWritableTuple();

		// Get all the <id,boolean,parent> of cases in boundary columns
		// Return <<idcase,boolean>;parentUpdated>
		public void reduce(
				Text uselessKey,
				Iterator<LongBooleanLongWritableTuple> idAndBooleanAndParentCases,
				OutputCollector<LongBooleanWritableTuple, LongWritable> output,
				Reporter reporter) throws IOException {
//			// TODO Plug the code of Sean Correctly
//			UnionFind uf = new UnionFind(idAndBooleanAndParentCases);
//
//			while (idAndBooleanAndParentCases.hasNext()) {
//				LongBooleanLongWritableTuple tuple = idAndBooleanAndParentCases
//						.next();
//				outputKey.set(tuple.l, tuple.b);
//				output.collect(outputKey,
//						new LongWritable(uf.getMyMostSouthWestParent(tuple.l)));
//			}
		}
	}

	public JobConf createFirstPassConf(String[] args) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_firstPass");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(MapFirstPass.class);
		conf.setCombinerClass(ReduceFirstPass.class);
		conf.setReducerClass(ReduceFirstPass.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-size".equals(args[i])) {
				conf.setInt("connectedcomponentscounter.matrix.size",
						new Integer(args[++i]));
			}
			if ("-columnWidth".equals(args[i])) {
				conf.setInt("connectedcomponentscounter.matrix.columnWidth",
						new Integer(args[++i]));
			} else {
				other_args.add(args[i]);
			}
		}

		// TODO Change them
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		return conf;
	}

	public JobConf createSecondPassConf(String[] args) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_secondPass");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(MapSecondPass.class);
		conf.setCombinerClass(ReduceSecondPass.class);
		conf.setReducerClass(ReduceSecondPass.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-size".equals(args[i])) {
				conf.setInt("connectedcomponentscounter.matrix.size",
						new Integer(args[++i]));
			}
			if ("-columnWidth".equals(args[i])) {
				conf.setInt("connectedcomponentscounter.matrix.columnWidth",
						new Integer(args[++i]));
			} else {
				other_args.add(args[i]);
			}
		}

		// TODO Change them
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		return conf;
	}

	public JobConf createThirdPassConf(String[] args) {
		JobConf conf = new JobConf(getConf(), ConnectedComponentsCounter.class);
		conf.setJobName("connectedComponentCounter_secondPass");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(MapThirdPass.class);
		conf.setCombinerClass(ReduceThirdPass.class);
		conf.setReducerClass(ReduceThirdPass.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-size".equals(args[i])) {
				conf.setInt("connectedcomponentscounter.matrix.size",
						new Integer(args[++i]));
			}
			if ("-columnWidth".equals(args[i])) {
				conf.setInt("connectedcomponentscounter.matrix.columnWidth",
						new Integer(args[++i]));
			} else {
				other_args.add(args[i]);
			}
		}

		// TODO Change them
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		return conf;
	}

	public int run(String[] args) throws Exception {

		Job firstPass = new Job(createFirstPassConf(args));
		Job secondPass = new Job(createSecondPassConf(args));
		Job thirdPass = new Job(createThirdPassConf(args));

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