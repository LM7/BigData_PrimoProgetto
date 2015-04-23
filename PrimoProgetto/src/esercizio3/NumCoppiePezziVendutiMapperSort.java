package esercizio3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumCoppiePezziVendutiMapperSort extends 
Mapper<Object, Text, PairWritable, IntWritable> {


	private PairWritable pair = new PairWritable();
	private IntWritable uno = new IntWritable(1);

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		ArrayList<String> riga = new ArrayList<String>();
		StringTokenizer words = new StringTokenizer(value.toString(),",");
		if(words.hasMoreTokens())
			words.nextToken();
		while(words.hasMoreTokens()) {
			String word = words.nextToken();
			riga.add(word);
		}
		for(int i=0;i<riga.size();i++)
			for(int j=i+1;j<riga.size();j++){
				pair.set(riga.get(i),riga.get(j));
				context.write(pair, uno);
			}
	}
}
