package session12.assignment2.q1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperNYC extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = value.toString().split("\t");

			Text hospital = new Text(lineArray[0]);
			IntWritable answer = new IntWritable(Integer.parseInt(lineArray[3]));

			context.write(hospital, answer);

		

	}

}
