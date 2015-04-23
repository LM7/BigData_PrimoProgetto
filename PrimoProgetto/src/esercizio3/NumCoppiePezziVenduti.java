package esercizio3;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NumCoppiePezziVenduti extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "num coppie");
	    job.setJarByClass(NumCoppiePezziVenduti.class);
	    job.setMapperClass(NumCoppiePezziVendutiMapperSort.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(NumCoppiePezziVendutiReducerSort.class);
	    job.setOutputKeyClass(PairWritable.class);
	    job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));	

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
			fs.delete(new Path(args[1]),true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
		
	
	public static void main(String[] args) throws Exception {
		String[] arg = new String[]{"words.txt","result3"};
		int exitCode = ToolRunner.run(new NumCoppiePezziVenduti(), arg);
		System.out.println("DONE "+exitCode);
	}
	
}
