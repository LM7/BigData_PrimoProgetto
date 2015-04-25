package esercizioB;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InsiemiProdottiCardReducer extends MapReduceBase 
		implements Reducer<EnneWritable, IntWritable, EnneWritable, IntWritable> {
	
	private IntWritable somma = new IntWritable();
	
	public void reduce(EnneWritable key, Iterator<IntWritable> values, 
		OutputCollector<EnneWritable, IntWritable> output, Reporter reporter)
	throws IOException {
		
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		somma.set(sum);
		output.collect(key, somma);
	}
}