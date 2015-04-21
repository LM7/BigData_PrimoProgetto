package esercizio1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NumPezziVendutiMapperSort extends MapReduceBase 
		implements Mapper<Object, Text, IntWritable, Text> {
	
	private Text letter = new Text();
	private IntWritable num = new IntWritable();
	
	public void map(Object key, Text value, 
			OutputCollector<IntWritable,Text> output, Reporter reporter) 
	throws IOException {
		
		// Break line into words for processing
		StringTokenizer wordList = new StringTokenizer(value.toString(),"\n");
		
		while (wordList.hasMoreTokens()) {
			StringTokenizer words = new StringTokenizer(wordList.nextToken());
			//if(words.hasMoreTokens())
				//words.nextToken();
			while(words.hasMoreTokens()) {
				String word = words.nextToken();
				int numero = Integer.parseInt(words.nextToken());
				letter.set(word);
				num.set(numero*(-1));
				output.collect(num, letter);
			}
			
		}
	}
}
