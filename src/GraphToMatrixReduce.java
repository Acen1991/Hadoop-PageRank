import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class GraphToMatrixReduce extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mout;

	public void setup(Context context) {
		mout = new MultipleOutputs<Text, Text>(context);
	}

	public void cleanup(Context context) throws IOException {
		try {
			mout.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<Text> successors = new ArrayList<Text>();
		Text returnValue = new Text("");
		Text returnKey = new Text("");
		String i = key.toString();

		Iterator<Text> j = values.iterator();
		while (j.hasNext()) {
			successors.add(new Text(j.next()));
		}

		Configuration conf = context.getConfiguration();

		long N = conf.getLong("numNodes", 0);
		double invN = 1.0 / N;

		int M = successors.size();
		for (Text successor : successors) {
			if (M != 0) {
				double invM = 1.0 / M;
				returnKey = new Text(successor + " " + i);
				returnValue = new Text(invM + "");
				mout.write("matrix", returnKey, returnValue,
						"stochasticMatrix/");
			}
		}
		returnKey = new Text(i);
		returnValue = new Text(invN + "");
		mout.write("vector", returnKey, returnValue, "initialPageRank/");
	}
}