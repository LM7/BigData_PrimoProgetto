package esercizio2;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class VenditePrmTrimestreReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable somma = new IntWritable();
	private String sottoChiave = "";
	//private String interaChiave = "";
	
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
	throws IOException {
		
		
		int sum = 0;
		//System.out.println("KEY: "+key.toString());
		String prodotto = key.toString();
		String[] veroP = prodotto.split(" ");
		String[] prova= new String[3];
		prova[0] = veroP[0];
		prova[1] = veroP[1];
		//System.out.println("prodotto: "+ prova[0]);
		//System.out.println("data: "+ prova[1]);
		while (values.hasNext()) {
			sum += values.next().get();
		}
		
		somma.set(sum);
		
		prova[2] = String.valueOf(somma);
		
		
		//System.out.println("somma: "+prova[2]);
		//System.out.println("PROVA0: "+prova[0]+";sottoCHIAVE: "+sottoChiave);
		if (prova[0].equals(sottoChiave)) {
			key = new Text("	"+prova[1]);//key = new Text(interaChiave + " " +prova[1]);
		}
		output.collect(key, somma);
		sottoChiave = prova[0];
		//interaChiave = prodotto+ " "+prova[2];
		//System.out.println("SCRITTO");
		//System.out.println("-------------------------------------");
		
		
	}
}