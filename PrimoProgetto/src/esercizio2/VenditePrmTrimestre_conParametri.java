package esercizio2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VenditePrmTrimestre_conParametri extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		JobConf conf = new JobConf(getConf(), VenditePrmTrimestre_conParametri.class);
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
		//int exitCode = ToolRunner.run(new AverageWordLength(), args);
		String[] arg = new String[]{"products.txt","result"};
		int exitCode = ToolRunner.run(new VenditePrmTrimestre_conParametri(), arg);
		//System.exit(exitCode);
		System.out.println("DONE "+exitCode);
	}
}

