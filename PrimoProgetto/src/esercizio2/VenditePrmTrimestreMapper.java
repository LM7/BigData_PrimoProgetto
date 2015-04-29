package esercizio2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VenditePrmTrimestreMapper extends Mapper<Object, Text, Text, IntWritable> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
    private Text data = new Text();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		
	    	StringTokenizer itr2 = new StringTokenizer(value.toString(), ",");
	    	data.set(itr2.nextToken());
	    	while(itr2.hasMoreTokens()) {
	    		Calendar cal = Calendar.getInstance();
	    		String dataStringa = data.toString();
	    		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
	    		try {
					Date date = formatter.parse(dataStringa);
					cal.setTime(date);
					int anno = cal.get(Calendar.YEAR);
					int mese = cal.get(Calendar.MONTH) + 1; 
					String annoStringa = String.valueOf(anno);
					String meseStringa = String.valueOf(mese);
					String dataStringaNuova = meseStringa + "/" + annoStringa;
			    	word.set(itr2.nextToken(","));
			   		//String prodottoStringa = word.toString();
			   		String finale = word + " "+dataStringaNuova+":";
			   		Text finalissimo = new Text(finale);
			   		if (mese <= 3) {
			   			context.write(finalissimo, one);
			   			
			   			
			   		}
			   		
				} 
	    		catch (ParseException e) {
					e.printStackTrace();
				}
	    		
	    		
	    	}
	      
	}
}

