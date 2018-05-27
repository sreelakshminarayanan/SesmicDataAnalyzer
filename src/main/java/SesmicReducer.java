
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SesmicReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException,
			InterruptedException {
		float max = 0;

		// Go through all values to sum up card values for a card suit
		for (FloatWritable value : values) {
			if ( value.get() > max) {
				max = value.get();
			}
		}

		context.write(key, new FloatWritable(max));
	}
}
