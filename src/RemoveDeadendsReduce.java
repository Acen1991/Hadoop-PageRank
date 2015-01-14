import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class RemoveDeadendsReduce extends Reducer<Text, Text, LongWritable, Text> {

	protected void reduce(Text key,Iterable<Text> value, Context context) throws IOException, InterruptedException {
		ArrayList<Text> successors = new ArrayList<Text>();
		ArrayList<Text> predecessors = new ArrayList<Text>();

		Iterator<Text>  iterator = value.iterator();
		while(iterator.hasNext())
		{
			String[] tab = iterator.next().toString().split("\\s");
			if(tab.length>1)
			{
				String successorOrPrecedecessors = tab[1];
				if(successorOrPrecedecessors.equals("s"))
				{
					successors.add(new Text(tab[0]));
				} else
				{
					predecessors.add(new Text(tab[0]));
				}
			}
		}

		Counter c = context.getCounter(MyCounters.NUMNODES);

		// use the counter when the actual node doesn't have any successor
		if((successors.size()==0))// || (successors.get(key).get(0).equals(key) && successors.get(key).size()==1))  //this is added if we consider that the loop that ended as deadend are deadends
		{
			c.increment(1);
		}
		else 
		{
			for(Text v : predecessors)
			{
				String ijString =   v.toString() + "\t" + key.toString();
				Text ij = new Text(ijString);
				context.write(null, ij);
			}
		}
	}
}