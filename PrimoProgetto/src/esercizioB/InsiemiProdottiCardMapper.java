package esercizioB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

public class InsiemiProdottiCardMapper extends MapReduceBase 
implements Mapper<Object, Text, EnneWritable, IntWritable> {

	EnneWritable pair = new EnneWritable();
	EnneWritable triple = new EnneWritable();
	EnneWritable quadruple = new EnneWritable();

	private IntWritable uno = new IntWritable(1);

	@Override
	public void map(Object key, Text value, OutputCollector<EnneWritable, IntWritable> output, Reporter reporter) 
			throws IOException {

		ArrayList<String> riga = new ArrayList<String>();
		StringTokenizer words = new StringTokenizer(value.toString(),",");
		if(words.hasMoreTokens())
			words.nextToken();
		while(words.hasMoreTokens()) {
			String word = words.nextToken();
			riga.add(word);
		}
		
		for(int i=0;i<riga.size();i++) {
			for(int j=i+1;j<riga.size();j++) {
				Text[] tmp1= new Text[]{new Text(riga.get(i)),new Text(riga.get(j)),new Text(" "),new Text(" ")};
				//crea coppie
				pair.set(tmp1);
				output.collect(pair, uno);
				for(int w=j+1;w<riga.size();w++) {
					Text[] tmp2= new Text[]{new Text(riga.get(i)),new Text(riga.get(j)),new Text(riga.get(w)),new Text(" ")};
					//crea triple
					triple.set(tmp2);
					output.collect(triple, uno);
					for(int k=w+1;k<riga.size();k++){
						Text[] tmp3= new Text[]{new Text(riga.get(i)),new Text(riga.get(j)),new Text(riga.get(w)),new Text(riga.get(k))};
						//crea quadruple
						quadruple.set(tmp3);
						output.collect(quadruple, uno);
					}
				}
			}
		}
		
	}
}
