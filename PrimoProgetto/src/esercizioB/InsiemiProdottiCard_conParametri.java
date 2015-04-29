package esercizioB;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InsiemiProdottiCard_conParametri extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Path inputFile = new Path(args[0]);
		Path outputTemp = new Path(args[1]);

		JobConf conf = new JobConf(getConf(), InsiemiProdottiCard_conParametri.class);
		conf.setJobName("InsiemiProdottiCard_conParametri");

		FileInputFormat.setInputPaths(conf, inputFile);	

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputTemp)){
			fs.delete((outputTemp),true);
		}

		FileOutputFormat.setOutputPath(conf, outputTemp);
		conf.setMapperClass(InsiemiProdottiCardMapper.class);
		conf.setReducerClass(InsiemiProdottiCardReducer.class);
		conf.setMapOutputKeyClass(EnneWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(EnneWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
		
		return 0;
	}


	public static void main(String[] args) throws Exception {
		String[] arg = new String[]{"products.txt","resultB"};
		int exitCode = ToolRunner.run(new InsiemiProdottiCard_conParametri(), arg);
		System.out.println("Operation DONE, exitCode: "+exitCode);
		System.exit(exitCode);
	}


}
