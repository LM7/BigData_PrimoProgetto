package esercizio1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NumPezziVendutiSort extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		JobConf conf = new JobConf(getConf(), NumPezziVendutiSort.class);
		conf.setJobName("AverageWordLength");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(NumPezziVendutiMapperSort.class);
		conf.setReducerClass(NumPezziVendutiReducerSort.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf);
		return 0;
	}
	/*	
	public static void main(String[] args) throws Exception {
		//int exitCode = ToolRunner.run(new AverageWordLength(), args);
		String[] arg = new String[]{"result/part-00000","resultCOPIA"};
		int exitCode = ToolRunner.run(new NumPezziVendutiCOPIA(), arg);
		//System.exit(exitCode);
		System.out.println("DONE "+exitCode);
	}
	*/
}
