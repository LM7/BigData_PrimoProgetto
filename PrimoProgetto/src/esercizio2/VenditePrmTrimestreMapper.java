package esercizio2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class VenditePrmTrimestreMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
    //private Text prodotto = new Text();
    private Text data = new Text();
	
	public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		
		StringTokenizer itr1 = new StringTokenizer(value.toString(), "\n");
	      while (itr1.hasMoreTokens()) {
	    	StringTokenizer itr2 = new StringTokenizer(itr1.nextToken(), ",");
	    	data.set(itr2.nextToken());
	    	//System.out.println("DATA: "+data);
	    	while(itr2.hasMoreTokens()) {
	    		Calendar cal = Calendar.getInstance();
	    		String dataStringa = data.toString();
	    		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
	    		try {
					Date date = formatter.parse(dataStringa);
					cal.setTime(date);
					int anno = cal.get(Calendar.YEAR);
					int mese = cal.get(Calendar.MONTH) + 1; //perchè sempre minore di uno??
					String annoStringa = String.valueOf(anno);
					String meseStringa = String.valueOf(mese);
					String dataStringaNuova = meseStringa + "/" + annoStringa;
			    	word.set(itr2.nextToken(","));
			   		//String prodottoStringa = word.toString();
			   		String finale = word + " "+dataStringaNuova+":";
			   		Text finalissimo = new Text(finale);
			   		//System.out.println(finalissimo);
			   		if (mese <= 3) {
			   			//context.write(word, one);
			   			//System.out.println(finalissimo+" "+one);
			   			output.collect(finalissimo, one);
			   			
			   			
			   		}
			   		
				} 
	    		catch (ParseException e) {
					e.printStackTrace();
				}
	    		
	    		
	    	}
	      }
	}
}

