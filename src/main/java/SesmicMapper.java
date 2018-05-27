
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SesmicMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString();
		System.out.println("Input lines is "+ inputLine);
		String[] sesmicData = value.toString().split(",");
		System.out.println("Magnittude is "  + sesmicData[8]);
		context.write(new Text(sesmicData[0]), new FloatWritable(Float.parseFloat(sesmicData[8])));

	}
}