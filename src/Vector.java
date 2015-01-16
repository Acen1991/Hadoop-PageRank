import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class Vector {
	ArrayList<Double> coeffs;
	String vectorPath;
	Logger log = Logger.getLogger(Vector.class);

	public void setVector(Vector other) {
		this.coeffs = other.getCoefficients();
	}

	public void setVectorPath(String vectorPath) {
		this.vectorPath = vectorPath;
	}

	public Vector(String vectorPath) throws NumberFormatException, IOException {
		this.vectorPath = vectorPath;
		coeffs = new ArrayList<Double>();
		toArray(vectorPath);
	}

	public Vector(String dirPath, Boolean b) throws IOException {
		File outputFileR = openFile(dirPath);

		BufferedReader bufferedReadR = new BufferedReader(new FileReader(
				outputFileR));
		String line_r;
		coeffs = new ArrayList<Double>();

		while ((line_r = bufferedReadR.readLine()) != null) {
			String[] value = line_r.split("\\s+");
			coeffs.add(Double.valueOf(value[1]));
		}
		bufferedReadR.close();
	}

	private File openFile(String directory) {
		//Open the directory that holds to matricial product H*r_old
		//this repertory contains the "_SUCCESS" file, some hidden files and the "part-r-00000" file
		File newDirectory = new File(directory);
		if (!newDirectory.exists()) {
			log.warn("Output " + directory + " doesn't exist");
		}

		File[] contents = newDirectory.listFiles();
		File outputFile = null;

		for (int i = 0; i < contents.length; i++) {
			if (!contents[i].getName().equals("_SUCCESS")
					&& !contents[i].getName().startsWith(".")) {
				outputFile = contents[i].getAbsoluteFile();
			}
		}

		if (outputFile == null) {
			log.warn(outputFile + " file doesn't exist");
		}

		log.info("+++ IN OPEN FILE +++ directory = " + directory
				+ "  AND outputFile.getAbsolutePath() = "
				+ outputFile.getAbsolutePath());
		return outputFile;
	}

	public Vector(ArrayList<Double> coeff) {
		this.coeffs = coeff;
	}

	public ArrayList<Double> getCoefficients() {
		return coeffs;
	}

	public void write(String vectorDir) throws IOException {
		// some things to overwrite file
		PrintWriter pw = new PrintWriter(vectorDir + "/" + "rts", "UTF-8");
		for (int i = 0; i < coeffs.size(); i++) {
			String str = (i + 1) + "\t" + coeffs.get(i);
			log.info("asp newMatrixVector -->" + str);
			pw.println(str);
		}

		pw.close();
	}

	public void toArray(String vectorPath) throws NumberFormatException,
			IOException {
		SortedMap<Integer, Double> treeVector = new TreeMap<Integer, Double>();
		BufferedReader vectorBuffer = new BufferedReader(new FileReader(
				new File(vectorPath)));
		String line;
		while ((line = vectorBuffer.readLine()) != null) {
			String[] parts = line.split("\\s+");
			if (parts.length > 1)
				treeVector.put(Integer.parseInt(parts[0]),
						Double.parseDouble(parts[1]));
		}

		for (Integer i : treeVector.keySet()) {
			coeffs.add(treeVector.get(i));
		}

		vectorBuffer.close();
	}

	public void addWith(double value) {
		int l = coeffs.size();
		for (int i = 0; i < l; i++) {
			coeffs.set(i, coeffs.get(i) + value);
		}
	}

	public void multiplyWith(double value) {
		int l = coeffs.size();
		for (int i = 0; i < l; i++) {
			coeffs.set(i, coeffs.get(i) * value);
		}
	}

	public Vector soustractTo(Vector other) throws NumberFormatException,
			IOException {
		ArrayList<Double> otherCoeff = other.getCoefficients();
		ArrayList<Double> result = new ArrayList<Double>();

		for (int i = 0; i < otherCoeff.size(); i++) {
			result.add(coeffs.get(i) - otherCoeff.get(i));
		}
		return new Vector(result);
	}

	public static double dist(Vector v1, Vector v2)
			throws NumberFormatException, IOException {
		return norm1(v1.soustractTo(v2));
	}

	public static double norm1(Vector vect) {
		double norm = 0.0;
		for (Double coeff : vect.getCoefficients())
			norm += Math.abs(coeff);
		return norm;
	}
}