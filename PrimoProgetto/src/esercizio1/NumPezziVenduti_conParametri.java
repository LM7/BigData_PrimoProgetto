package esercizio1;

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

public class NumPezziVenduti_conParametri extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 3) {
			System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		JobConf conf = new JobConf(getConf(), NumPezziVenduti_conParametri.class);
		conf.setJobName("NumPezziVenduti");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));	

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
			fs.delete(new Path(args[1]),true);
		}
		
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(NumPezziVendutiMapper.class);
		conf.setReducerClass(NumPezziVendutiReducer.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf);
		
		JobConf conf2 = new JobConf(getConf(), NumPezziVendutiSort.class);
		conf2.setJobName("NumPezziVendutiSort");

		FileInputFormat.setInputPaths(conf2, args[1]+"/part-00000");		
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		conf2.setMapperClass(NumPezziVendutiMapperSort.class);
		conf2.setReducerClass(NumPezziVendutiReducerSort.class);
		conf2.setMapOutputKeyClass(IntWritable.class);
		conf2.setMapOutputValueClass(Text.class);
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf2);
		
		return 0;
	}
		
	public static void main(String[] args) throws Exception {
		String[] arg = new String[]{"products.txt","result","resultSort"};
		int exitCode = ToolRunner.run(new NumPezziVenduti_conParametri(), arg);
		System.out.println("First Operation DONE, exitCode: "+exitCode);
		System.exit(exitCode);
	}

}