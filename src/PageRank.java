import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {

	//--------------------------------------------first job----------------------------------------
	enum myCounters{ 
		NUMNODES;
	}

	static class RemoveDeadendsMap extends Mapper<LongWritable,Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//TODO
			String[] nodes = value.toString().split("\\t");
			String i = nodes[0];
			String j = nodes[1];

			context.write(new Text(i),new Text(j+ " " + "s"));
			context.write(new Text(j),new Text(i+ " " + "p"));
		}
	}


	static class RemoveDeadendsReduce extends Reducer<Text, Text, LongWritable, Text> {

		protected void reduce(Text key,Iterable<Text> value, Context context) throws IOException, InterruptedException {
			//TODO
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

			Counter c = context.getCounter(myCounters.NUMNODES);

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

	public static void removeDeadendsJob(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{	
		boolean existDeadends = true;
		String intermediaryDir = conf.get("intermediaryDirPath");
		String input = conf.get("processedGraphPath");
		long nNodes = conf.getLong("numNodes", 0);
		Path inputPath = new Path(input);

		FileSystem fs = FileSystem.get(conf);
		int k = 0;
		Path intermediaryDirPath = new Path(intermediaryDir);

		while(existDeadends)
		{
			intermediaryDirPath = new Path(intermediaryDir);
			//TODO
			/* in the input you will have the original graph. At the end, the result should be in the same path(processedGraphPath). The result is the graph without the edges of the deadends. this graph is constructed iterativly, until we 
			don't have deadends left. intermediaryDir is used for the iteration steps
			use the counter to check the stop condition */
			nNodes = conf.getLong("numNodes", 0);

			Job job1 = Job.getInstance(conf);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setMapperClass(RemoveDeadendsMap.class);
			job1.setReducerClass(RemoveDeadendsReduce.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job1, inputPath);
			FileOutputFormat.setOutputPath(job1, intermediaryDirPath);
			job1.waitForCompletion(true);

			long c = job1.getCounters().findCounter(myCounters.NUMNODES).getValue();
			conf.setLong("numNodes", nNodes-c);
			long nNodesAfter = conf.getLong("numNodes", 0);

			if(nNodes != nNodesAfter) 	
			{
				fs.delete(new Path(input),true);
				fs.rename(intermediaryDirPath, new Path(input));
			}
			else 
			{
				existDeadends = false;
			}
		}
	}

	//--------------------------------------------second job----------------------------------------
	// you can change the types defined for the input and output, but make sure to do the changes in all the necessary places
	static class GraphToMatrixMap extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//TODO
			String[] nodes = value.toString().split("\\s+");
			String i = nodes[0];
			String j = nodes[1];

			//System.out.println("i = " + i + " et j = " +j);
			context.write(new Text(i), new Text(j));
		}
	}

	static class GraphToMatrixReduce extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text,Text> mout;
		public void setup(Context context) {
			mout = new MultipleOutputs<Text,Text>(context);
		}
		public void cleanup(Context context) throws IOException {
			try {
				mout.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<Text,ArrayList<Text>> successors = new HashMap<Text,ArrayList<Text>>();
			Text returnValue = new Text("");
			Text returnKey = new Text("");
			String i = key.toString();

			Iterator<Text> j = values.iterator();
			while(j.hasNext())
			{
				ArrayList<Text> temp = (successors.get(key)==null) ? new ArrayList<Text>() : successors.get(key);
				temp.add(new Text(j.next()));
				successors.put(key, temp);
			}

			Configuration conf= context.getConfiguration();

			long N = conf.getLong("numNodes", 0);
			double invN = 1.0/N;

			for(Text node : successors.keySet())
			{
				for(Text successor : successors.get(node))
				{
					int M = successors.get(node).size();
					if(M!=0)
					{
						double invM = 1.0/M;
						returnKey = new Text(successor+" "+i);
						returnValue = new Text(invM+"");
						mout.write("matrix", returnKey , returnValue, "stochasticMatrix/" );  
					}
				}
				returnKey = new Text(i);
				returnValue = new Text(invN+"");
				mout.write("vector", returnKey, returnValue, "initialPageRank/");
			}
		}
	} 

	public static void GraphToMatrixJob(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(GraphToMatrixMap.class);
		job.setReducerClass(GraphToMatrixReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		MultipleOutputs.addNamedOutput(job, "vector", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "matrix", TextOutputFormat.class,
				Text.class, Text.class);
		FileInputFormat.setInputPaths(job, new Path(conf.get("processedGraphPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("multipleOutputsPath")));
		job.waitForCompletion(true);
	}

	//--------------------------------------------Iteration-------------------------------------/

	public static void avoidSpiderTraps(String vectorDir, long nNodes, double beta) throws IOException
	{
		//TODO
		Vector vect = new Vector(vectorDir,true);
		vect.multiplyWith(beta);
		vect.addWith((1-beta)/nNodes);
		vect.write(vectorDir);
	}


	public static boolean checkConvergence(String initialDir, String iterationDir, double epsilon) throws NumberFormatException, IOException	{
		Vector initalVect = new Vector(initialDir,true);
		Vector iterationVect = new Vector(iterationDir,true); 
		//TODO 

		System.out.println("==== In checkConvergence ====");
		return (Vector.dist(initalVect,iterationVect)<epsilon);
	}



	public static void iterativePageRank(Configuration conf) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		String initialVector = conf.get("initialPageRankPath");
		String currentVector = conf.get("currentPageRankPath");

		/* here is were the final result should be */
		String finalVector = conf.get("finalPageRankPath");
		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

		PageRank.removeDeadendsJob(conf);

		long nNodes = conf.getLong("numNodes", 1);

		PageRank.GraphToMatrixJob(conf);
		//TODO
		//Definition of the iterative pagerank job, take care of what you'll write here
		boolean convergence = false; // variable verifiant la convergence
		boolean converge = false;
		int nbIterations = 0;

		while(!convergence){
			int k = (++nbIterations);
			System.out.println("========= Iteration " + k + " ==========");
			conf.set("currentPageRankPath", currentVector + "/" + k ); //a chaque iteration, on change le directory du currentVector
			String currentVectorIterationK = conf.get("currentPageRankPath");

			MatrixVectorMult.multiplicationJob(conf); // resultat stocker dans currentPageRankPath/k

			avoidSpiderTraps(currentVectorIterationK,nNodes,beta); //reecrire le resultat dans currentPageRankPath/k
			converge = checkConvergence(initialVector, currentVectorIterationK, epsilon);	
			if(converge)
			{
				convergence = true;
				FileUtils.deleteDirectory(new File(finalVector));
				FileUtils.copyDirectory(new File(currentVectorIterationK), new File(finalVector));

			} else {
				convergence = false;
				FileUtils.deleteDirectory(new File(initialVector));
				FileUtils.copyDirectory(new File(currentVectorIterationK), new File(initialVector));
			}
		} 
	}

	public static class Vector
	{
		ArrayList<Double> coeffs;
		String vectorPath;

		public void setVector(Vector other)
		{
			this.coeffs = other.getCoefficients();
		}

		public void setVectorPath(String vectorPath)
		{
			this.vectorPath=vectorPath;
		}

		public Vector(String vectorPath) throws NumberFormatException, IOException
		{
			this.vectorPath = vectorPath;
			coeffs = new ArrayList<Double>();
			toArray(vectorPath);
		}

		public Vector(String dirPath, Boolean b) throws IOException
		{
			File outputFileR = openFile(dirPath);

			BufferedReader bufferedReadR = new BufferedReader(new FileReader(outputFileR));
			String line_r;
			coeffs = new ArrayList<Double>();

			while((line_r = bufferedReadR.readLine()) != null){
				String[] value = line_r.split("\\s+");
				coeffs.add(Double.valueOf(value[1]));
			}
			bufferedReadR.close();
		}

		private File openFile(String directory){
			// Ouvrir le repertoire contenant le produit matriciel de H*r_old
			// ce repertoire contient le fichier _SUCCESS, fichiers caches et part-r-00000
			File newDirectory = new File(directory);
			if(!newDirectory.exists()){
				System.out.println("Output " + directory + " doesn't exist");
			}

			File[] contents = newDirectory.listFiles();
			File outputFile = null;

			for (int i = 0; i < contents.length; i++){
				if (!contents[i].getName().equals("_SUCCESS")&& !contents[i].getName().startsWith(".") ){
					outputFile = contents[i].getAbsoluteFile();
				}
			}

			if(outputFile == null){
				System.out.println(outputFile + " file doesn't exist");
			}

			System.out.println("+++ IN OPEN FILE +++ directory = "+directory+"  AND outputFile.getAbsolutePath() = " + outputFile.getAbsolutePath());
			return outputFile;
		}

		public Vector(ArrayList<Double> coeff)
		{
			this.coeffs = coeff;
		}

		public ArrayList<Double> getCoefficients()
		{
			return coeffs;
		}

		public void write(String vectorDir) throws IOException
		{
			//some things to overwrite file
			PrintWriter pw = new PrintWriter(vectorDir + "/" + "rts", "UTF-8");
			for(int i = 0; i < coeffs.size(); i++)
			{
				String str = (i+1) + "\t" + coeffs.get(i);
				System.out.println("asp newMatrixVector -->" + str);
				pw.println(str);
			}

			pw.close();
		}

		public void toArray(String vectorPath) throws NumberFormatException, IOException
		{
			SortedMap<Integer,Double> treeVector = new TreeMap<Integer,Double>();
			//TODO 
			BufferedReader vectorBuffer = new BufferedReader(new FileReader(new File(vectorPath)));
			String line;
			while ((line = vectorBuffer.readLine()) != null) {
				String[] parts = line.split("\\s+");
				if(parts.length>1) treeVector.put(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));		
			}

			for(Integer i : treeVector.keySet())
			{
				coeffs.add(treeVector.get(i));
			}

			vectorBuffer.close();
		}

		public void addWith(double value)
		{
			int l = coeffs.size();
			for(int i = 0; i<l; i++)
			{
				coeffs.set(i,coeffs.get(i)+value);
			}
		}

		public void multiplyWith(double value)
		{
			int l = coeffs.size();
			for(int i = 0; i<l; i++)
			{
				coeffs.set(i,coeffs.get(i)*value);
			}
		}

		public Vector soustractTo(Vector other) throws NumberFormatException, IOException
		{
			ArrayList<Double> otherCoeff = other.getCoefficients();
			ArrayList<Double> result = new ArrayList<Double>();

			for(int i = 0; i < otherCoeff.size(); i++)
			{
				result.add(coeffs.get(i)-otherCoeff.get(i));
			}
			return new Vector(result);
		}

		public static double dist(Vector v1, Vector v2) throws NumberFormatException, IOException
		{
			return norm1(v1.soustractTo(v2));
		}

		public static double norm1(Vector vect)
		{
			double norm = 0.0;
			for(Double coeff: vect.getCoefficients())
				norm+=Math.abs(coeff);
			return norm;
		}
	}
}
