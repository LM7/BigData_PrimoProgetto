package esercizio1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NumPezziVendutiReducerSort extends MapReduceBase 
		implements Reducer<IntWritable, Text, Text, IntWritable> {
	
	private static final String SEP = ",";
	
	public void reduce(IntWritable key, Iterator<Text> values, 
		OutputCollector<Text,IntWritable> output, Reporter reporter)
	throws IOException {
		
		//output.collect(key, values.next());
		StringBuilder valueList = new StringBuilder();
		boolean firstValue = true;
		
		while (values.hasNext()) {
			output.collect(new Text(values.next()),new IntWritable(key.get()*(-1)));
		}
		
	}
}