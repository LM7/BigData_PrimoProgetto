package esercizioA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoppieVenduteInsiemeReducerSort extends 
Reducer<CoppieWritable, IntWritable, CoppieWritable, DoubleWritable> {

	//private static final int TOP_K = 10;
	private class Pair_num {
		public String str_left;
		public String str_right;
		public Integer count;

		public Pair_num(String str_left, String str_right, Integer count) {
			this.str_left = str_left;
			this.str_right = str_right;
			this.count = count;
		}
	};
	private PriorityQueue<Pair_num> queue;
	private HashMap<String, Integer> prodotto2occorrenze = new HashMap<String, Integer>();


	@Override
	protected void reduce(CoppieWritable key, Iterable<IntWritable> values, 
			Context ctx) throws IOException, InterruptedException {

		int sum = 0;
		for(IntWritable val: values) {
			sum += val.get();
		}
		if(!key.getRightPair().toString().equals("NULL"))
			queue.add(new Pair_num(key.getLeftPair().toString(), 
					key.getRightPair().toString(), sum));
		else
			prodotto2occorrenze.put(key.getLeftPair().toString(), sum);
	}


	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Pair_num>(10,new Comparator<Pair_num>() {
			public int compare(Pair_num p1, Pair_num p2) {
				return p1.count.compareTo(p2.count);
			}
		});
	}

	@Override
	protected void cleanup(Context ctx) 
			throws IOException, InterruptedException {
		List<Pair_num> topKPairs = new ArrayList<Pair_num>();
		while (! queue.isEmpty()) {
			topKPairs.add(queue.remove());
		}
		for (int i = topKPairs.size() - 1; i >= 0; i--) {
			Pair_num topKPair = topKPairs.get(i);
			int occ = prodotto2occorrenze.get(topKPair.str_left);
			ctx.write(new CoppieWritable(new Text(topKPair.str_left), new Text(topKPair.str_right)), 
					new DoubleWritable(
							Math.floor( ((double)topKPair.count/(double)occ)*100 ) /100));
		}
	}
}