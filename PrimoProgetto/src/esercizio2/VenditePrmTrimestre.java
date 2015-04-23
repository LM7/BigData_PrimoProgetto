package esercizio2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VenditePrmTrimestre extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		JobConf conf = new JobConf(getConf(), VenditePrmTrimestre.class);
		conf.setJobName("venditePrmTrimestre");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));	

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
			fs.delete(new Path(args[1]),true);
		}
		
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(VenditePrmTrimestreMapper.class);
		conf.setReducerClass(VenditePrmTrimestreReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
		return 0;
	}
		
	public static void main(String[] args) throws Exception {
		System.out.println(args.length);
		if (args.length != 2) {
			System.out.printf("Usage: VenditePrmTrimestre /path/to/product.txt output_dir");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}
		int exitCode = ToolRunner.run(new Configuration(), new VenditePrmTrimestre(), args);
		System.out.println("Operations DONE, exitCode: "+exitCode);
		System.exit(exitCode); 
	}
}

