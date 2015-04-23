package esercizio1;

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

public class NumPezziVenduti extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Path inputFile = new Path(args[0]);
		Path outputTemp = new Path(args[1]);
		Path outputFinal = new Path(args[2]);

		JobConf conf = new JobConf(getConf(), NumPezziVenduti.class);
		conf.setJobName("NumPezziVenduti");

		FileInputFormat.setInputPaths(conf, inputFile);	

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputTemp)){
			fs.delete((outputTemp),true);
		}

		FileOutputFormat.setOutputPath(conf, outputTemp);
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
		FileOutputFormat.setOutputPath(conf2, outputFinal);
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
		System.out.println(args.length);
		if (args.length != 3) {
				System.out.printf("Usage: NumPezziVenduti /path/to/product.txt output_dir");
				System.exit(-1);
			}
		int exitCode = ToolRunner.run(new Configuration(),new NumPezziVenduti(), args);
		System.out.println("Operations DONE, exitCode: "+exitCode);
		System.exit(exitCode); 
	}


}
