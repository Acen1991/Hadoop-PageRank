import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RemoveDeadendsMap extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] nodes = value.toString().split("\\t");
		String i = nodes[0];
		String j = nodes[1];

		context.write(new Text(i), new Text(j + " " + "s"));
		context.write(new Text(j), new Text(i + " " + "p"));
	}
}