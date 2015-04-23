package esercizio3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumCoppiePezziVendutiReducerSort extends 
Reducer<PairWritable, IntWritable, PairWritable, IntWritable> {

	private static final int TOP_K = 10;
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


	@Override
	protected void reduce(PairWritable key, Iterable<IntWritable> values, 
			Context ctx) throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable val: values) {
			sum += val.get();
		}
		
		queue.add(new Pair_num(key.getLeftPair().toString(), 
				key.getRightPair().toString(), sum));
		
		if (queue.size() > TOP_K) {
			queue.remove();
		}
	}

	
	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Pair_num>(TOP_K, new Comparator<Pair_num>() {
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
			ctx.write(new PairWritable(new Text(topKPair.str_left), new Text(topKPair.str_right)), 
					new IntWritable(topKPair.count));
		}
	}
}