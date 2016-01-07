package io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;

public class SnapToVertexInputFormatFromEachLineConverter {

	static final String INPUT_FILE1 = "files/smallworld_1.txt";
	static final String INPUT_FILE2 = "files/smallworld_2.txt";
	static final String OUTPUT_FILE = "files/smallworld_hdfs.txt";
	static final int NUMBER_OF_LABELS = 50;
	static final long NUMBER_OF_NODES = 50000000;


	public static void main(String[] args)
	{
		try {
			double zipfDistributionDenominator = 0;
			for (int k = 1; k <= NUMBER_OF_LABELS; k++) zipfDistributionDenominator += 1.0 / k;
			double[] zipfDistribution = new double[NUMBER_OF_LABELS + 1];
			zipfDistribution[0] = 0;
			for (int k = 1; k <= NUMBER_OF_LABELS; k++) zipfDistribution[k] = zipfDistribution[k - 1] + (1.0 / k) / zipfDistributionDenominator;
			PrintWriter out = new PrintWriter(OUTPUT_FILE);
			BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream("files/smallworld_1.txt")));
			BufferedReader br2 = new BufferedReader(new InputStreamReader(new FileInputStream("files/smallworld_2.txt")));
			String line1 = br1.readLine();
			String line2 = br2.readLine();
			for (long i = 0; i < NUMBER_OF_NODES; i++)
			{
				double thisNodesOutEdgeProb = Math.random();
				Set<Long> outEdges = new HashSet<Long>();
				Set<Long> inEdges = new HashSet<Long>();
				Object[] vertexArray = new Object[4];

				while (line1 != null)
				{
					long line1From = Long.parseLong(line1.split("\t")[0]);
					long line1To = Long.parseLong(line1.split("\t")[1]);
					if (line1From == i) {
						if (Math.random() < thisNodesOutEdgeProb) outEdges.add(line1To); else inEdges.add(line1To); 
						line1 = br1.readLine(); 
					}
					else break;
				}

				while (line2 != null)
				{
					long line2From = Long.parseLong(line2.split("\t")[0]);
					long line2To = Long.parseLong(line2.split("\t")[1]);
					if (line2To == i) {
						if (Math.random() < thisNodesOutEdgeProb) outEdges.add(line2From); else inEdges.add(line2From); 
						line2 = br2.readLine(); 
					}
					else break;
				}
				int labelNumber = -1 * Arrays.binarySearch(zipfDistribution, Math.random()) - 1;
				
				Object[] vertexData = new Object[2];
				vertexData[0] = i;
				vertexData[1] = labelNumber;
				
				vertexArray[0] = i;
				vertexArray[1] = vertexData;
				vertexArray[2] = outEdges.toArray();
				vertexArray[3] = inEdges.toArray();
				
				out.println(new Gson().toJson(vertexArray));

			}

			// Done with the file
			br1.close();
			br2.close();
			out.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
