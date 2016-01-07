package io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class GraphCoarsening {

	static final String INPUT_FILE = "/home/dm/walaa/research/snap/examples/graphgen/smallworld.txt";
	static final int NUMBER_OF_NODES = 50000000;
	static final String OUTPUT_FILE = "/home/dm/walaa/research/snap/examples/graphgen/smallworld.txt.metis";
	static int COMPRESSION_ITERATIONS = 4;
	static final int NUM_METIS_PARTITIONS = 100;
	static int[] sequentialIds = new int[NUMBER_OF_NODES];


	public static void main(String[] args) throws Exception
	{
		//for (int NUM_METIS_PARTITIONS = 20; NUM_METIS_PARTITIONS <= 20; NUM_METIS_PARTITIONS +=10)
			//for (int COMPRESSION_ITERATIONS = 0; COMPRESSION_ITERATIONS <= 4; COMPRESSION_ITERATIONS++)
			{
				long t1,t2;
				t1=System.currentTimeMillis();
				File inputFile = new File(INPUT_FILE);
				String inputFileAbsoluteFile = inputFile.getAbsoluteFile().toString();
				String inputFileDirectory = inputFile.getParentFile().getAbsolutePath();
				String inputFileName = inputFile.getName();
				File tmpDir = new File(inputFileDirectory, UUID.randomUUID().toString());
				tmpDir.mkdir();

				File fileSortedByFirstCol = new File(tmpDir, inputFileName + "_1");
				File fileSortedBySecondCol = new File(tmpDir, inputFileName + "_2");

				ProcessBuilder b1 = new ProcessBuilder("tail", "-n", "+5", inputFileAbsoluteFile);
				b1.redirectOutput(fileSortedByFirstCol);
				Process p1 = b1.start();
				p1.waitFor();

				ProcessBuilder b2 = new ProcessBuilder("sort", "-n", "-k", "2", fileSortedByFirstCol.toString());
				b2.redirectOutput(fileSortedBySecondCol);
				Process p2 = b2.start();
				p2.waitFor();

				t2=System.currentTimeMillis();
				//System.out.println("Done Sorting " + (t2-t1));
				t1=t2;

				int[][] graph = new int[NUMBER_OF_NODES][];
				BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(fileSortedByFirstCol)));
				BufferedReader br2 = new BufferedReader(new InputStreamReader(new FileInputStream(fileSortedBySecondCol)));
				String line1 = br1.readLine();
				String line2 = br2.readLine();
				for (int i = 0; i < NUMBER_OF_NODES; i++)
				{
					Set<Integer> edges = new HashSet<Integer>();

					////System.out.println("*****");
					while (line1 != null)
					{
						////System.out.println(line1);
						int line1From = Integer.parseInt(line1.split("\t")[0]);
						int line1To = Integer.parseInt(line1.split("\t")[1]);
						if (line1From == i) {
							edges.add(line1To); 
							line1 = br1.readLine(); 
						}
						else break;
					}
					while (line2 != null)
					{
						////System.out.println(line2);
						int line2From = Integer.parseInt(line2.split("\t")[0]);
						int line2To = Integer.parseInt(line2.split("\t")[1]);
						if (line2To == i) {
							edges.add(line2From); 
							line2 = br2.readLine(); 
						}
						else break;
					}
					//if (i==10) System.exit(0);

					int numberOfNeighbors = edges.size();
					graph[i] = new int[numberOfNeighbors];
					int j = 0;
					for (int n : edges) graph[i][j++] = n;
				}
				br1.close();
				br2.close();
				fileSortedByFirstCol.delete();
				fileSortedBySecondCol.delete();

				int[] superNodes = new int[NUMBER_OF_NODES];
				int[][] originalGraph = new int[NUMBER_OF_NODES][];
				for (int i = 0; i < NUMBER_OF_NODES; i++)
				{
					originalGraph[i] = new int[graph[i].length];
					System.arraycopy(graph[i], 0, originalGraph[i], 0, graph[i].length);
				}

				t2=System.currentTimeMillis();
				//System.out.println("Done Loading Graph " + (t2-t1));
				t1=t2;

				createMetisFile(originalGraph, OUTPUT_FILE + ".orig");

				t2=System.currentTimeMillis();
				//System.out.println("Done creating original Metis file " + (t2-t1));
				t1=t2;

				for (int k = 0; k < COMPRESSION_ITERATIONS; k++)
				{
					////System.out.println(Arrays.deepToString(graph));
					int[] matchings = new int[NUMBER_OF_NODES];
					boolean[] marked = new boolean[NUMBER_OF_NODES];

					for (int i = 0; i < NUMBER_OF_NODES; i++)
					{
						if (graph[i] == null) continue;
						if (!marked[i])
						{
							float maxJaccard = 0;
							int maxJaccardNeighbor = i;
							for (int j : graph[i])
							{
								if (!marked[j])
								{
									float jaccard = getJaccard(graph[i], graph[j]);
									if (jaccard >= maxJaccard) { maxJaccard = jaccard; maxJaccardNeighbor = j; }
								}
							}
							//maxJaccardNeighbor = graph[i][new Random().nextInt(graph[i].length)];
							matchings[i] = maxJaccardNeighbor;
							matchings[maxJaccardNeighbor] = i;
							marked[i] = true;
							marked[maxJaccardNeighbor] = true;
							superNodes[i] = min(i, maxJaccardNeighbor);
							superNodes[maxJaccardNeighbor] = min(i, maxJaccardNeighbor);
						}

					}
					for (int i = 0; i < NUMBER_OF_NODES; i++)
						superNodes[i] = superNodes[superNodes[i]];

					////System.out.println("Matchings: " + Arrays.toString(matchings));
					////System.out.println("Super Nodes: " + Arrays.toString(superNodes));
					int[][] coarsendGraph = new int[NUMBER_OF_NODES][];

					for (int i = 0; i < NUMBER_OF_NODES; i++)
					{
						int coarsenedNode = min(i, matchings[i]);
						if (i != coarsenedNode) continue;
						Set<Integer> neighbors = new HashSet<Integer>();
						for (int j : graph[i])
							neighbors.add(superNodes[j]);
						for (int j : graph[matchings[i]])
							neighbors.add(superNodes[j]);
						neighbors.remove(coarsenedNode);
						int numberOfNeighbors = neighbors.size();
						coarsendGraph[coarsenedNode] = new int[numberOfNeighbors];
						int j = 0;
						for (int n : neighbors) coarsendGraph[coarsenedNode][j++] = n;
					}

					////System.out.println(Arrays.deepToString(coarsendGraph));
					graph = coarsendGraph;
					t2=System.currentTimeMillis();
					//System.out.println("Done coarsening step " + (t2-t1));
					t1=t2;
				}

				int numOfCoarsenedGraphNodes = createMetisFile(graph, OUTPUT_FILE);

				t2=System.currentTimeMillis();
				//System.out.println("Done creating Metis input file " + (t2-t1));
				t1=t2;

				if (numOfCoarsenedGraphNodes == 1) {
					//System.out.println("Cannot partition a single-node graph. Exiting.");
					return;
				}
				String outputFileAbsoluteFile = new File(OUTPUT_FILE).getAbsoluteFile().toString();
				ProcessBuilder b3 = new ProcessBuilder("/usr/local/bin/gpmetis", outputFileAbsoluteFile, String.valueOf(NUM_METIS_PARTITIONS));
				Process p3 = b3.start();
				p3.waitFor();

				t2=System.currentTimeMillis();
				//System.out.println("Done Metis partitioning " + (t2-t1));
				t1=t2;

				String metisPartitionsFile = outputFileAbsoluteFile + ".part." + NUM_METIS_PARTITIONS;
				BufferedReader br3 = new BufferedReader(new InputStreamReader(new FileInputStream(metisPartitionsFile)));
				String line3;
				int[] metisPartitions = new int[numOfCoarsenedGraphNodes];
				int n = 0;
				while ((line3 = br3.readLine()) != null)
				{
					int partition = Integer.parseInt(line3);
					metisPartitions[n++] = partition;
				}
				br3.close();

				int[] uncoarsenedPartitions1;
				int[] uncoarsenedPartitions2;
				if (COMPRESSION_ITERATIONS > 0)
				{
					uncoarsenedPartitions1 = new int[NUMBER_OF_NODES];
					uncoarsenedPartitions2 = new int[NUMBER_OF_NODES];
					for (int i = 0; i < NUMBER_OF_NODES; i++)
					{
						if (graph[i] == null) continue;
						uncoarsenedPartitions1[i] = metisPartitions[sequentialIds[i]];
					}
					for (int i = 0; i < NUMBER_OF_NODES; i++)
						uncoarsenedPartitions2[i] = uncoarsenedPartitions1[superNodes[i]];
				}
				else
					uncoarsenedPartitions2 = metisPartitions;
				//System.out.println(NUM_METIS_PARTITIONS + "\t" + COMPRESSION_ITERATIONS + "\t" + getEdgeCut(originalGraph, uncoarsenedPartitions2) + "\t" + getCommunicationVolume(originalGraph, uncoarsenedPartitions2));
			}
	}

	static int min(int a, int b)
	{
		return a < b ? a : b;
	}
	static float getJaccard(int [] a, int[] b)
	{
		Set<Integer> as = new HashSet<Integer>();
		for (int e : a) as.add(e);
		Set<Integer> bs = new HashSet<Integer>();
		for (int e : b) bs.add(e);
		Set<Integer> intersection = new HashSet<Integer>(as);
		intersection.retainAll(bs);
		Set<Integer> union = new HashSet<Integer>(as);
		union.addAll(bs);
		return (float)intersection.size() / union.size();
	}

	static int createMetisFile(int[][] graph, String outputFile) throws Exception
	{
		int sequentialId = 0;
		long numberOfEdges = 0;
		for (int i = 0; i < NUMBER_OF_NODES; i++)
		{
			if (graph[i] == null) continue;
			sequentialIds[i] = sequentialId++;
			numberOfEdges += graph[i].length;
		}

		PrintWriter out = new PrintWriter(outputFile);
		out.println(sequentialId + " " + numberOfEdges / 2);
		for (int i = 0; i < NUMBER_OF_NODES; i++)
		{
			if (graph[i] == null) continue;
			StringBuilder out_line = new StringBuilder();
			for (int n : graph[i])
				out_line.append(sequentialIds[n] + 1 + " ");
			if (out_line.length() > 0) out_line.deleteCharAt(out_line.length() - 1);
			out.println(out_line.toString());
		}
		out.flush();
		out.close();
		return sequentialId;	
	}

	static long getCommunicationVolume(int[][] graph, int[] vertexPartition)
	{
		long communicationVolume = 0;
		for (int i = 0; i < NUMBER_OF_NODES; i++)
		{
			Set<Integer> otherPartitions = new HashSet<Integer>(); 
			for (int j : graph[i])
				if (vertexPartition[i]!=vertexPartition[j]) otherPartitions.add(vertexPartition[j]);
			communicationVolume += otherPartitions.size();
		}
		return communicationVolume;
	}

	static long getEdgeCut(int[][] graph, int[] vertexPartition)
	{
		long edgecut = 0;
		for (int i = 0; i < NUMBER_OF_NODES; i++)
			for (int j : graph[i])
				if (vertexPartition[i]!=vertexPartition[j]) edgecut++;
		return edgecut / 2;
	}


}
