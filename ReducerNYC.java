package session12.assignment2.q1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerNYC extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int answerPC = Integer.MIN_VALUE;
		for(IntWritable value: values) {
			if(value.get() > answerPC)
				answerPC = value.get();
		}
		
		context.write(key, new IntWritable(answerPC));
	}

}
