import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;


/**
 * This class contains the public test cases for the project. The public tests
 * are distributed with the project description, and students can run the public
 * tests themselves against their own implementation.
 * 
 * Any changes to this file will be ignored when testing your project.
 * 
 */
public class PublicTests extends BaseTests {

	static {

		try {
			Configuration conf = new Configuration();
			// First job
			conf.set("initialGraphPath", "data/initialGraph");
			conf.set("processedGraphPath", "data/processedGraph");
			conf.set("initialPageRankPath", "data/mo/initialPageRank");
			conf.set("currentPageRankPath", "data/mo/currentPageRank");
			conf.set("stochasticMatrixPath", "data/mo/stochasticMatrix");
			conf.set("finalPageRankPath", "data/finalVector");
			conf.set("multipleOutputsPath", "data/mo");
			conf.set("intermediaryDirPath", "data/intermediary");

			FileUtils.deleteDirectory(new File("data/intermediary"));
			FileUtils.deleteDirectory(new File("data/finalVector"));
			FileUtils.deleteDirectory(new File("data/processedGraph"));
			FileUtils.deleteDirectory(new File("data/mo"));
			FileUtils.copyDirectory(new File("data/initialGraph"), new File("data/processedGraph"));
			conf.setLong("numNodes", 6);
			conf.setDouble("epsilon", 0.1);
			conf.setDouble("beta", 0.8);
			PageRank.iterativePageRank(conf);
		} catch (IOException e) {
			initializationError = e.toString();
			System.out.println(initializationError);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void testFinalVector(){
		try {
			File directory = new File("data/finalVector");
			if(!directory.exists())
				fail("Output directory doesn't exist");

			File[] contents = directory.listFiles();
			File outputFile = null;

			for (int i = 0; i < contents.length; ++i)
				if (!contents[i].getName().equals("_SUCCESS")
						&& !contents[i].getName().startsWith("."))
					outputFile = contents[i].getAbsoluteFile();

			if (outputFile == null)
				fail("Output file doesn't exist");

			BufferedReader r;

			r = new BufferedReader(new FileReader(outputFile));

			double sum = 0.;
			double value = 0.;
			String line;
			while ((line = r.readLine()) != null) {
				String[] parts = line.split("\\s+");
				if(parts.length < 2)
					return;
				if(Integer.valueOf(parts[0]) == 3)
					value = Double.valueOf(parts[1]);
				sum += Double.valueOf(parts[1]);
			}

			r.close();
			assertEquals(0.15, value, 0.01);
			assertEquals(1, sum, 0.01);

		} catch (IOException e) {
			System.out.println(e.toString());
			fail(e.toString());
		}
	}
	public void testRemoveDeadendsJob(){
		try {
			Configuration conf = new Configuration();
			// First job
			conf.set("initialGraphPath", "data/initialGraph");
			conf.set("processedGraphPath", "data/processedGraph");
			conf.set("intermediaryDirPath", "data/intermediary");
			FileUtils.deleteDirectory(new File("data/intermediary"));
			FileUtils.deleteDirectory(new File("data/processedGraph"));
			FileUtils.copyDirectory(new File("data/initialGraph"), new File("data/processedGraph"));

			conf.setLong("numNodes", 6);
			PageRank.removeDeadendsJob(conf);

			File directory = new File("data/processedGraph");
			if(!directory.exists())
				fail("Output directory doesn't exist");

			File[] contents = directory.listFiles();
			File outputFile = null;

			for (int i = 0; i < contents.length; ++i)
				if (!contents[i].getName().equals("_SUCCESS")
						&& !contents[i].getName().startsWith("."))
					outputFile = contents[i].getAbsoluteFile();

			if (outputFile == null)
				fail("Output file doesn't exist");

			BufferedReader r = new BufferedReader(new FileReader(outputFile));
			int noLines = 0;
			String line;
			while ((line = r.readLine()) != null) {
				String[] parts = line.split("\\s+");
				noLines ++;
				if(Integer.valueOf(parts[0]) == 2 && Integer.valueOf(parts[0]) == 5)
					fail("DeadEnd still exists");
			}

			r.close();
			assertEquals(9, noLines);
			assertEquals(4, conf.getLong("numNodes", 0));

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			System.out.println(e.toString());
			fail(e.toString());
		}
	}

	public void testConstructionInitialVector(){
		try {
			Configuration conf = new Configuration();
			// First job
			conf.set("processedGraphPath", "data/testProcessedGraph");
			conf.set("multipleOutputsPath", "data/mo");
			conf.set("initialPageRankPath", "data/mo/initialPageRank");
			conf.set("stochasticMatrixPath", "data/mo/stochasticMatrix");
			FileUtils.deleteDirectory(new File("data/mo"));


			conf.setLong("numNodes", 3);
			PageRank.GraphToMatrixJob(conf);

			File directory = new File("data/mo/initialPageRank");
			if(!directory.exists())
				fail("Output directory data/mo/initialPageRank doesn't exist");

			File[] contents = directory.listFiles();
			File outputFile = null;

			for (int i = 0; i < contents.length; ++i)
				if (!contents[i].getName().equals("_SUCCESS")
						&& !contents[i].getName().startsWith("."))
					outputFile = contents[i].getAbsoluteFile();

			if (outputFile == null)
				fail("Output file data/mo/initialPageRank/part-r-000000 doesn't exist");

			BufferedReader r = new BufferedReader(new FileReader(outputFile));
			int noLines = 0;
			double sum = 0.;
			String line;
			while ((line = r.readLine()) != null) {
				String[] parts = line.split("\\s+");
				if(parts.length < 2)
					continue;
				noLines ++;
				sum += Double.parseDouble(parts[1]);
			}

			r.close();
			if(noLines != 3)
				fail("Number of elements of the initial vector is not correct");
			assertEquals(1.0, sum, 0.01);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			System.out.println(e.toString());
			fail(e.toString());
		}
	}

	public void testConstructionStochasticMatrix(){
		try {
			Configuration conf = new Configuration();
			// First job
			conf.set("processedGraphPath", "data/testProcessedGraph");
			conf.set("multipleOutputsPath", "data/mo");
			conf.set("initialPageRankPath", "data/mo/initialPageRank");
			conf.set("stochasticMatrixPath", "data/mo/stochasticMatrix");
			FileUtils.deleteDirectory(new File("data/mo"));


			conf.setLong("numNodes", 3);
			PageRank.GraphToMatrixJob(conf);

			File directory = new File("data/mo/stochasticMatrix");
			if(!directory.exists())
				fail("Output directory data/mo/stochasticMatrix doesn't exist");

			File[] contents = directory.listFiles();
			File outputFile = null;

			for (int i = 0; i < contents.length; ++i)
				if (!contents[i].getName().equals("_SUCCESS")
						&& !contents[i].getName().startsWith("."))
					outputFile = contents[i].getAbsoluteFile();

			if (outputFile == null)
				fail("Output file data/mo/stochasticMatrix/part-r-000000 doesn't exist");

			HashMap<Integer, Double> sumColumns = new HashMap<Integer, Double>();
			BufferedReader r = new BufferedReader(new FileReader(outputFile));
			int noLines = 0;
			String line;
			while ((line = r.readLine()) != null) {
				String[] parts = line.split("\\s+");
				if(parts.length < 3)
					continue;
				noLines ++;
				if(sumColumns.get(Integer.valueOf(parts[1])) == null)
					sumColumns.put(Integer.valueOf(parts[1]), Double.valueOf((parts[2])));
				else
					sumColumns.put(Integer.valueOf(parts[1]), sumColumns.get(Integer.valueOf(parts[1])) + Double.valueOf(parts[2]));
			}

			r.close();
			if(noLines != 6)
				fail("Number of elements of the matrix is not correct");
			for(int k:sumColumns.keySet())
				if(sumColumns.get(k) != 1)
					fail("Matrix is not stochastic");

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			System.out.println(e.toString());
			fail(e.toString());
		}
	}
}

