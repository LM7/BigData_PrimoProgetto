package esercizio1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumeroPezziVendutiMapperSort extends 
Mapper<Object, Text, Text, IntWritable> {

	private IntWritable uno = new IntWritable(1);

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		StringTokenizer words = new StringTokenizer(value.toString(),",");
		if(words.hasMoreTokens())
			words.nextToken();
		while(words.hasMoreTokens()) {
			String word = words.nextToken();
			Text wordText = new Text(word);
			context.write(wordText, uno);
		}
	}
}
