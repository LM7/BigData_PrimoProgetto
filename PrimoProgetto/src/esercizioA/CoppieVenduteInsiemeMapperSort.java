package esercizioA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoppieVenduteInsiemeMapperSort extends 
Mapper<Object, Text, CoppieWritable, IntWritable> {


	private CoppieWritable pair = new CoppieWritable();
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
		for(int i=0;i<riga.size();i++){
			pair.set(riga.get(i),"NULL");
			context.write(pair, uno);
			for(int j=0;j<riga.size();j++){
				if(i!=j){
					pair.set(riga.get(i),riga.get(j));
					context.write(pair, uno);
				}
			}
		}
	}
}
