package esercizioA;

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


public class CoppieVenduteInsieme extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "num coppie");
	    job.setJarByClass(CoppieVenduteInsieme.class);
	    job.setMapperClass(CoppieVenduteInsiemeMapperSort.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(CoppieVenduteInsiemeReducerSort.class);
	    job.setOutputKeyClass(CoppieWritable.class);
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
		if (args.length != 2) {
			System.out.printf("Usage: CoppieVenduteInsieme /path/to/product.txt output_dir");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}
		int exitCode = ToolRunner.run(new Configuration(), new CoppieVenduteInsieme(), args);
		System.out.println("Operations DONE, exitCode: "+exitCode);
		System.exit(exitCode);
	}
	
}
