package esercizio2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VenditePrmTrimestreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable somma = new IntWritable();
	//private String sottoChiave = "";
	//private String interaChiave = "";
	private HashMap<String, ArrayList<String[]>> prod2date_occ = new HashMap<String,ArrayList<String[]>>();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
		
		
		
		int sum = 0;
		String interaChiave = key.toString();
		String[] arrayInteraChiave = interaChiave.split(" ");
		String prodotto = arrayInteraChiave[0];
		String data = arrayInteraChiave[1];
		
		
		for(IntWritable val: values) {
			sum += val.get();
		}
		
		somma.set(sum);
		String valore = String.valueOf(somma);
		
		String[] data_occ = new String[2];
		data_occ[0] = data;
		data_occ[1] = valore;
		ArrayList<String[]> appoggio = new ArrayList<String[]>();
		
		
		
		if (prod2date_occ.containsKey(prodotto)) {
			appoggio = prod2date_occ.get(prodotto);
			appoggio.add(data_occ);
		}
		else {
			appoggio.add(data_occ);
			prod2date_occ.put(prodotto, appoggio );
		}
		
		
		
	}
	
	protected void cleanup(Context ctx) throws IOException, InterruptedException {
		for (String prodotto: prod2date_occ.keySet()) {
			String prod_data_occ = prodotto;
			ArrayList<String[]> lista_prodotto = prod2date_occ.get(prodotto);
			int i;
			for (i = 0; i<lista_prodotto.size()-1; i++) {
				prod_data_occ += " "+lista_prodotto.get(i)[0]+" "+lista_prodotto.get(i)[1];
			}
			prod_data_occ += " "+lista_prodotto.get(i)[0];
			String stringaSommaValore = lista_prodotto.get(i)[1];
			int sommaValore = Integer.parseInt(stringaSommaValore);
			Text key = new Text(prod_data_occ);
			IntWritable val = new IntWritable(sommaValore);
			ctx.write(key, val);
		}	
		
	}
}