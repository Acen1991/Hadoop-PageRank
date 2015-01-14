import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {

	// --------------------------------------------first
	// job----------------------------------------
	public static void removeDeadendsJob(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		boolean existDeadends = true;
		String intermediaryDir = conf.get("intermediaryDirPath");
		String input = conf.get("processedGraphPath");
		long nNodes = conf.getLong("numNodes", 0);
		Path inputPath = new Path(input);

		FileSystem fs = FileSystem.get(conf);
		int k = 0;
		Path intermediaryDirPath = new Path(intermediaryDir);

		while (existDeadends) {
			intermediaryDirPath = new Path(intermediaryDir);
			/*
			 * in the input you will have the original graph. At the end, the
			 * result should be in the same path(processedGraphPath). The result
			 * is the graph without the edges of the deadends. this graph is
			 * constructed iterativly, until we don't have deadends left.
			 * intermediaryDir is used for the iteration steps use the counter
			 * to check the stop condition
			 */
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

			long c = job1.getCounters().findCounter(MyCounters.NUMNODES)
					.getValue();
			conf.setLong("numNodes", nNodes - c);
			long nNodesAfter = conf.getLong("numNodes", 0);

			if (nNodes != nNodesAfter) {
				fs.delete(new Path(input), true);
				fs.rename(intermediaryDirPath, new Path(input));
			} else {
				existDeadends = false;
			}
		}
	}

	// --------------------------------------------second
	// job----------------------------------------
	public static void GraphToMatrixJob(Configuration conf) throws IOException,
			ClassNotFoundException, InterruptedException {
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
		FileInputFormat.setInputPaths(job,
				new Path(conf.get("processedGraphPath")));
		FileOutputFormat.setOutputPath(job,
				new Path(conf.get("multipleOutputsPath")));
		job.waitForCompletion(true);
	}

	// --------------------------------------------Iteration-------------------------------------/
	public static void avoidSpiderTraps(String vectorDir, long nNodes,
			double beta) throws IOException {
		Vector vect = new Vector(vectorDir, true);
		vect.multiplyWith(beta);
		vect.addWith((1 - beta) / nNodes);
		vect.write(vectorDir);
	}

	public static boolean checkConvergence(String initialDir,
			String iterationDir, double epsilon) throws NumberFormatException,
			IOException {
		Vector initalVect = new Vector(initialDir, true);
		Vector iterationVect = new Vector(iterationDir, true);

		return (Vector.dist(initalVect, iterationVect) < epsilon);
	}

	public static void iterativePageRank(Configuration conf)
			throws NumberFormatException, IOException, ClassNotFoundException,
			InterruptedException {
		String initialVector = conf.get("initialPageRankPath");
		String currentVector = conf.get("currentPageRankPath");

		// here is were the final result should be
		String finalVector = conf.get("finalPageRankPath");
		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

		PageRank.removeDeadendsJob(conf);

		long nNodes = conf.getLong("numNodes", 1);

		PageRank.GraphToMatrixJob(conf);
		// Definition of the iterative pagerank job, take care of what you'll
		// write here
		// variable verifiant la convergence
		boolean convergence = false;
		boolean converge = false;
		int nbIterations = 0;

		while (!convergence) {
			int k = (++nbIterations);
			System.out.println("========= Iteration " + k + " ==========");
			conf.set("currentPageRankPath", currentVector + "/" + k);
			// a chaque iteration, on change le directory du currentVector
			String currentVectorIterationK = conf.get("currentPageRankPath");

			// resultat stocker dans currentPageRankPath/k
			MatrixVectorMult.multiplicationJob(conf);

			// reecrire le resultat dans currentPageRankPath/k
			avoidSpiderTraps(currentVectorIterationK, nNodes, beta);
			converge = checkConvergence(initialVector, currentVectorIterationK,
					epsilon);
			if (converge) {
				convergence = true;
				FileUtils.deleteDirectory(new File(finalVector));
				FileUtils.copyDirectory(new File(currentVectorIterationK),
						new File(finalVector));

			} else {
				convergence = false;
				FileUtils.deleteDirectory(new File(initialVector));
				FileUtils.copyDirectory(new File(currentVectorIterationK),
						new File(initialVector));
			}
		}
	}
}
