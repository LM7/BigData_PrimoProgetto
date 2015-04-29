package esercizio1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumeroPezziVendutiReducerSort extends 
Reducer<Text, IntWritable, Text, IntWritable> {

	private class Word_num {
		public String str;
		public Integer count;

		public Word_num(String str, Integer count) {
			this.str = str;
			this.count = count;
		}
	};
	private PriorityQueue<Word_num> queue;


	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
			Context ctx) throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable val: values) {
			sum += val.get();
		}
		queue.add(new Word_num(key.toString(), sum));
	}

	
	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Word_num> (1, new Comparator<Word_num>() {
			public int compare(Word_num p1, Word_num p2) {
				return p1.count.compareTo(p2.count);
			}
		});
	}

	@Override
	protected void cleanup(Context ctx) 
			throws IOException, InterruptedException {
		List<Word_num> words_nums = new ArrayList<Word_num>();
		while (! queue.isEmpty()) {
			words_nums.add(queue.remove());
		}
		for (int i = words_nums.size() - 1; i >= 0; i--) {
			Word_num word_num = words_nums.get(i);
			ctx.write(new Text(word_num.str), new IntWritable(word_num.count));
		}
	}
}