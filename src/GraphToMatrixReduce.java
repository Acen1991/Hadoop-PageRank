import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

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
		HashMap<Text, ArrayList<Text>> successors = new HashMap<Text, ArrayList<Text>>();
		Text returnValue = new Text("");
		Text returnKey = new Text("");
		String i = key.toString();

		Iterator<Text> j = values.iterator();
		while (j.hasNext()) {
			ArrayList<Text> temp = (successors.get(key) == null) ? new ArrayList<Text>()
					: successors.get(key);
			temp.add(new Text(j.next()));
			successors.put(key, temp);
		}

		Configuration conf = context.getConfiguration();

		long N = conf.getLong("numNodes", 0);
		double invN = 1.0 / N;

		for (Text node : successors.keySet()) {
			for (Text successor : successors.get(node)) {
				int M = successors.get(node).size();
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
}