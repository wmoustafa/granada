package io;

import it.unimi.dsi.fastutil.ints.Int2ByteMap;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.gson.Gson;

public class SnapToPartitionedGiraph1 {

    /*static final String SNAP_FILENAME = "/home/dm/walaa/research/snap/examples/graphgen/smallworld100.txt";
	static final int NUMBER_OF_NODES = 100;
	static final int NUM_LEVEL1_PARTITIONS = 2;
	static final int NUM_LEVEL2_PARTITIONS = 2;
	static final int NUM_LEVEL3_PARTITIONS = 4;
	static final int NUMBER_OF_LABELS = 5;
	static final String OUTPUT_FILE = "/home/dm/walaa/graph_files/smallworld100.dag.txt.hdfs";
	static final String TMP_DIR = "/home/dm/walaa/graph_files/tmp/metis";*/
    
        static String SNAP_FILENAME = "";
	static int NUMBER_OF_NODES = 50000000;
	static int NUM_LEVEL1_PARTITIONS = 69;
	static double SAMPLE_RATE = 1;
	static int NUM_LEVEL2_PARTITIONS = 69;
	static int NUM_LEVEL3_PARTITIONS = 102;
	static int NUMBER_OF_LABELS = 5;
    static boolean DAG = false;
    static String EDGE_WEIGHTS = "unit";
    static boolean USE_METIS = true;
    
	static String OUTPUT_FILE = "";
	static String OUTPUT_FILE_NO_EXT, CSV_FILE;
//	static final String TMP_DIR = "/home/dm/walaa/datasets/tmp/metis";
    static final String TMP_DIR = "/vicky/tmp";

	public static void main(String[] args) throws Exception
	{
	    SNAP_FILENAME = args[0];
	    SAMPLE_RATE = Float.parseFloat(args[1]);
		int[][] graph = sequentialize(getGraph(SNAP_FILENAME, 1300000000));
	    NUMBER_OF_NODES = graph.length;
	    NUM_LEVEL1_PARTITIONS = Integer.parseInt(args[2]);
        if (args[3].equals("0")) NUM_LEVEL2_PARTITIONS = NUM_LEVEL1_PARTITIONS; else NUM_LEVEL2_PARTITIONS = Integer.parseInt(args[3]);
        if (args[4].equals("0")) NUM_LEVEL3_PARTITIONS = (int)Math.sqrt(NUMBER_OF_NODES/NUM_LEVEL1_PARTITIONS/NUM_LEVEL2_PARTITIONS); else NUM_LEVEL3_PARTITIONS = Integer.parseInt(args[4]);
	    NUMBER_OF_LABELS = Integer.parseInt(args[5]);
	    EDGE_WEIGHTS = args[6];
	    DAG = Boolean.parseBoolean(args[7]);
	    USE_METIS = Boolean.parseBoolean(args[8]);
	    
	    String inputFilename = new File(SNAP_FILENAME).getName();
	    if(inputFilename.contains(".")) inputFilename = inputFilename.substring(0, inputFilename.lastIndexOf('.'));
	    

            //VP Simplify file names since only the first partition defines the number of super-vertices

	    OUTPUT_FILE_NO_EXT = "/vicky/outputs/" + inputFilename + "." + NUM_LEVEL1_PARTITIONS;

//	    OUTPUT_FILE_NO_EXT = "home/vpapavas/giraph_paper/granada/benchmarks/datasets/" + inputFilename + "." + NUM_LEVEL1_PARTITIONS + "." + NUM_LEVEL2_PARTITIONS
//		+ "." + NUM_LEVEL3_PARTITIONS + "." + EDGE_WEIGHTS + "." + NUMBER_OF_LABELS + (DAG? ".dag" : ".cyc") + (USE_METIS? ".metis" : ".range"); 

//	    OUTPUT_FILE_NO_EXT = "/home/dm/walaa/graph_files/" + inputFilename + "." + NUM_LEVEL1_PARTITIONS + "." + NUM_LEVEL2_PARTITIONS
//                + "." + NUM_LEVEL3_PARTITIONS + "." + EDGE_WEIGHTS + "." + NUMBER_OF_LABELS + (DAG? ".dag" : ".cyc") + (USE_METIS? ".metis" : ".range");

//	    OUTPUT_FILE = OUTPUT_FILE_NO_EXT + ".txt.hdfs"; 
	    OUTPUT_FILE = OUTPUT_FILE_NO_EXT + ".datalog.txt"; 
	    CSV_FILE = OUTPUT_FILE_NO_EXT + ".csv"; 
		File tmpDir = new File(TMP_DIR, UUID.randomUUID().toString());
		tmpDir.mkdirs();

		int[][] threeLevelPartitioning = new int[NUMBER_OF_NODES][3];

		System.out.println("Graph size = " + graph.length);
		int[] level1VertexPartitions = getNodePartitions(graph, NUM_LEVEL1_PARTITIONS,tmpDir);
		int[][][] level1Graphs = getGraphPartitions(graph, level1VertexPartitions, NUM_LEVEL1_PARTITIONS);

		assignThreeLevelPartitioning(graph, level1VertexPartitions, threeLevelPartitioning, 0);
		for (int i = 0; i < NUM_LEVEL1_PARTITIONS; i++)
		{
			int[] level2VertexPartitions = getNodePartitions(level1Graphs[i], NUM_LEVEL2_PARTITIONS, tmpDir);
			int[][][] level2Graphs = getGraphPartitions(level1Graphs[i], level2VertexPartitions, NUM_LEVEL2_PARTITIONS);

			assignThreeLevelPartitioning(level1Graphs[i], level2VertexPartitions, threeLevelPartitioning, 1);

			for (int j = 0; j < NUM_LEVEL2_PARTITIONS; j++)
			{
				int[] level3VertexPartitions = getNodePartitions(level2Graphs[j], NUM_LEVEL3_PARTITIONS, tmpDir);
				//int[][][] level3GraphPartitions = getGraphPartitions(level2Graphs[i], level2VertexPartitions, NUM_LEVEL2_PARTITIONS);

				assignThreeLevelPartitioning(level2Graphs[j], level3VertexPartitions, threeLevelPartitioning, 2);
			}
		}
		int[][][][] verticesInEachPartition = getVerticesInEachPartition(graph, threeLevelPartitioning, NUM_LEVEL1_PARTITIONS, NUM_LEVEL2_PARTITIONS, NUM_LEVEL3_PARTITIONS);
		writeHdfsFile(graph, OUTPUT_FILE, CSV_FILE, NUMBER_OF_LABELS, NUMBER_OF_NODES, verticesInEachPartition, threeLevelPartitioning, NUM_LEVEL1_PARTITIONS, NUM_LEVEL2_PARTITIONS, NUM_LEVEL3_PARTITIONS);

	}
	static int[][] sequentialize(int[][] graph)
	{
		int maxNodeValue = 1300000000;
		int sequentialId = 0;
		int numberOfNodes = graph.length;
		int[] sequentialIds = new int[maxNodeValue + 1];
		for (int i = 0; i < numberOfNodes; i++)
			if (graph[i]!=null) sequentialIds[graph[i][0]] = sequentialId++;

		int[][] outputGraph = new int[sequentialId][];
		System.out.println(outputGraph.length);
		for (int i = 0; i < numberOfNodes; i++)
		{
			if (graph[i]!=null)
			{
				outputGraph[sequentialIds[i]] = new int[graph[i].length];
				outputGraph[sequentialIds[i]][0] = sequentialIds[i];
				for (int j = 1; j < graph[i].length; j++)
					outputGraph[sequentialIds[i]][j] = sequentialIds[graph[i][j]];
			}
		}
		System.out.println("Sequentialized the graph");
		return outputGraph;		
	}
	
	static int[][][][] getVerticesInEachPartition(int[][] graph, int[][] threeLevelPartitioning, int numLevel1Partitioning, int numLevel2Partitioning, int numLevel3Partitioning)
	{
		int[][][][] vericesInEachPartition = new int[numLevel1Partitioning][numLevel2Partitioning][numLevel3Partitioning][];
		int[][][] numberOfVericesInEachPartition = new int[numLevel1Partitioning][numLevel2Partitioning][numLevel3Partitioning];
		int[][][] indexOfVericesInEachPartition = new int[numLevel1Partitioning][numLevel2Partitioning][numLevel3Partitioning];
		int numberOfNodes = graph.length;
		for (int i = 0; i < numberOfNodes; i++)
		{
			int level1Partition = threeLevelPartitioning[i][0];
			int level2Partition = threeLevelPartitioning[i][1];
			int level3Partition = threeLevelPartitioning[i][2];
					numberOfVericesInEachPartition[level1Partition][level2Partition][level3Partition]++;
		}
		for (int i = 0; i < numLevel1Partitioning; i++)
			for (int j = 0; j < numLevel2Partitioning; j++)
				for (int k = 0; k < numLevel3Partitioning; k++)
					vericesInEachPartition[i][j][k] = new int[numberOfVericesInEachPartition[i][j][k]];
		for (int i = 0; i < numberOfNodes; i++)
		{
			int level1Partition = threeLevelPartitioning[i][0];
			int level2Partition = threeLevelPartitioning[i][1];
			int level3Partition = threeLevelPartitioning[i][2];
			vericesInEachPartition[level1Partition][level2Partition][level3Partition][indexOfVericesInEachPartition[level1Partition][level2Partition][level3Partition]++] = i;
		}
		return vericesInEachPartition;
	}

	static void writeHdfsFile(int[][] graph, String outputFile, String csvFile, int numberOfLabels, int numberOfNodes, int[][][][] verticesInEachPartition, int[][] threeLevelPartitioning, int numLevel1Partitioning, int numLevel2Partitioning, int numLevel3Partitioning) throws Exception
	{
		double zipfDistributionDenominator = 0;
		for (int k = 1; k <= numberOfLabels; k++) zipfDistributionDenominator += 1.0 / k;
		double[] zipfDistribution = new double[numberOfLabels + 1];
		zipfDistribution[0] = 0;
		for (int k = 1; k <= numberOfLabels; k++) zipfDistribution[k] = zipfDistribution[k - 1] + (1.0 / k) / zipfDistributionDenominator;

		int MAX_EDGE_WEIGHT = 100;
		double zipfDistributionDenominator_edges = 0;
		for (int k = 1; k <= MAX_EDGE_WEIGHT; k++) zipfDistributionDenominator_edges += 1.0 / k;
		double[] zipfDistribution_edges = new double[MAX_EDGE_WEIGHT + 1];
		zipfDistribution_edges[0] = 0;
		for (int k = 1; k <= MAX_EDGE_WEIGHT; k++) zipfDistribution_edges[k] = zipfDistribution_edges[k - 1] + (1.0 / k) / zipfDistributionDenominator_edges;

		PrintWriter giraphOut = new PrintWriter(outputFile);
		PrintWriter csvOut = new PrintWriter(csvFile);
		Int2ByteMap[] edgeWeights = new Int2ByteMap[graph.length];
		for (int i = 0; i < numLevel1Partitioning; i++)
			for (int j = 0; j < numLevel2Partitioning; j++)
				for (int k = 0; k < numLevel3Partitioning; k++)
				{
					if (verticesInEachPartition[i][j][k].length == 0) continue; 
					Object[] superVertexArray = new Object[4];
					
					SuperVertexId superVertexId = new SuperVertexId(i, j, k, numLevel2Partitioning, numLevel3Partitioning);
					superVertexArray[0] = superVertexId.toArray();
					
					Object[] superVertexData = new Object[verticesInEachPartition[i][j][k].length];
					int c = 0;
					Set<SuperVertexId> superNeighbors = new HashSet<>();
					Set<List<Integer>> superNeighborsDetails = new HashSet<List<Integer>>(); 
					Random r = new Random();
					for (int n : verticesInEachPartition[i][j][k])
					{
						Set<int[]> outEdges = new HashSet<int[]>();
						Set<int[]> inEdges = new HashSet<int[]>();
						for (int e = 1; e < graph[n].length; e++)
						{
							int neighborId = graph[n][e];
							int[] neighborIdAndWeight = new int[2];
							neighborIdAndWeight[0] = neighborId;
							
							if (edgeWeights[n] == null || edgeWeights[n].get(neighborId) == 0)
							{
								if (EDGE_WEIGHTS.equals("unit")) neighborIdAndWeight[1] = 1;
								if (EDGE_WEIGHTS.equals("uniform")) neighborIdAndWeight[1] = 1 + r.nextInt(100);
								if (EDGE_WEIGHTS.equals("skewed_low")) neighborIdAndWeight[1] = -1 * Arrays.binarySearch(zipfDistribution_edges, Math.random()) - 1;
								if (EDGE_WEIGHTS.equals("skewed_high")) neighborIdAndWeight[1] = 101 - (-1 * Arrays.binarySearch(zipfDistribution_edges, Math.random()) - 1);
								if (edgeWeights[neighborId] == null) edgeWeights[neighborId] = new Int2ByteOpenHashMap();
								edgeWeights[neighborId].put(n, (byte)neighborIdAndWeight[1]);
							}
							else neighborIdAndWeight[1] = edgeWeights[n].get(neighborId);
							
							if (DAG || ((n ^ neighborId) & 0x01) == 1)
							
								{if (n > neighborId) outEdges.add(neighborIdAndWeight); else inEdges.add(neighborIdAndWeight);}
							else 
								{if (n < neighborId) outEdges.add(neighborIdAndWeight); else inEdges.add(neighborIdAndWeight);}
							
							int ni = threeLevelPartitioning[neighborId][0];
							int nj = threeLevelPartitioning[neighborId][1];
							int nk = threeLevelPartitioning[neighborId][2];
							SuperVertexId neighborSuperVertexId = new SuperVertexId(ni, nj, nk, numLevel2Partitioning, numLevel3Partitioning);
							//if (!superVertexId.equals(neighborSuperVertexId)) 
								//superNeighbors.add(neighborSuperVertexId);
							List<Integer> superNeighborDetail = new ArrayList<Integer>();
							superNeighborDetail.add(neighborId);
							superNeighborDetail.add(neighborSuperVertexId.getPartitionId());
							superNeighborDetail.add(neighborSuperVertexId.getVertexId());
							superNeighborsDetails.add(superNeighborDetail);
						}
						int labelNumber = -1 * Arrays.binarySearch(zipfDistribution, Math.random()) - 1;

						
						Object[] vertexData = new Object[2];
						vertexData[0] = n;
						vertexData[1] = labelNumber;
					
						Object[] vertexArray = new Object[3];
						vertexArray[0] = vertexData;
						vertexArray[1] = inEdges.toArray();
						vertexArray[2] = outEdges.toArray();
						
						csvOut.print("vertex\t" + n + "\t" + labelNumber + "\t");
						for (int[] outEdge : outEdges)
							csvOut.print("edge\t" + n + "\t" + outEdge[0] + "\t" + outEdge[1] + "\t");
						csvOut.println();
						superVertexData[c++] = vertexArray;
					}
					
					superVertexArray[1] = superVertexData;
					
					//Object[] superVertexNeighborsArray = new Object[superNeighbors.size()];
					int e = 0;
					//for (SuperVertexId level3Neighbor : superNeighbors)
						//superVertexNeighborsArray[e++] = level3Neighbor.toArray();
					//superVertexArray[2] = superVertexNeighborsArray;
					superVertexArray[3] = superNeighborsDetails;
					//System.out.println(new Gson().toJson(superVertexArray));
					try {
						giraphOut.println(new Gson().toJson(superVertexArray));
					}catch(java.lang.OutOfMemoryError exc){
						System.out.println("SuperVertexArray Size= " + c);
						System.out.println("SuperVertexArray[0] = " +
							superVertexArray[0]);
						System.out.println("SuperVertexArray[1][1] (inEdges) " +
							"= " +superVertexArray[0]);
					}
				}

		giraphOut.flush();
		giraphOut.close();	
		csvOut.flush();
		csvOut.close();
	}

	static void assignThreeLevelPartitioning(int[][] graph, int[] partitions, int[][] threeLevelPartitioning, int level)
	{
		int numberOfNodes = graph.length;
		for (int i = 0; i < numberOfNodes; i++)
		{
			int nodeId = graph[i][0];
			threeLevelPartitioning[nodeId][level] = partitions[i];
		}
	}


	static int[] getNodePartitions(int[][] graph, int numberOfPartitions, File tmpDir) throws Exception
	{
		if (!USE_METIS) return getNodePartitions(graph, numberOfPartitions);
		int numberOfNodes = graph.length;
		//System.out.println("Number of nodes in getNodePartitions = " + numberOfNodes);
		
		if (numberOfNodes == 0) return new int[0];
		int maxNodeValue = graph[numberOfNodes - 1][0];

		int sequentialId = 0;
		long numberOfEdges = 0;
		int[] sequentialIds = new int[maxNodeValue + 1];
		for (int i = 0; i < numberOfNodes; i++)
		{
			sequentialIds[graph[i][0]] = sequentialId++;
			numberOfEdges += (graph[i].length - 1);
		}

		File outputFile = new File(tmpDir, UUID.randomUUID().toString());
		PrintWriter out = new PrintWriter(outputFile);
		out.println(numberOfNodes + " " + numberOfEdges / 2);
		for (int i = 0; i < numberOfNodes; i++)
		{
			StringBuilder outputLine = new StringBuilder();
			for (int j = 1; j < graph[i].length; j++)
				outputLine.append(sequentialIds[graph[i][j]] + 1 + " ");
			if (outputLine.length() > 0) outputLine.deleteCharAt(outputLine.length() - 1);
			out.println(outputLine.toString());
		}
		out.flush();
		out.close();

		String absoluteOutputFilename = outputFile.getAbsoluteFile().toString();
		//ProcessBuilder b3 = new ProcessBuilder("/usr/local/bin/gpmetis", absoluteOutputFilename, String.valueOf(numberOfPartitions));
		
		//parmetis ~/research/snap/examples/graphgen/smallworld.txt.metis.orig 1 200 0.5 0.5 7 7
		try{
		ProcessBuilder b3 = new ProcessBuilder("/usr/local/bin/parmetis", absoluteOutputFilename, "1", String.valueOf(numberOfPartitions), "0.5", "0.5", "7", "7");
		b3.redirectErrorStream(true); 
		Process p3 = b3.inheritIO().start();
		p3.waitFor();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		String metisPartitionsFile = absoluteOutputFilename + ".part";
		//System.out.println(metisPartitionsFile);

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(metisPartitionsFile)));
		String line3;
		int[] nodePartitions = new int[numberOfNodes];
		int n = 0;
		while ((line3 = br.readLine()) != null)
		{
			int partition = Integer.parseInt(line3);
			nodePartitions[n++] = partition;
		}
		int[] partitionSizes = new int[numberOfPartitions];
		for (int i = 0; i < nodePartitions.length; i++)
			partitionSizes[nodePartitions[i]] += (graph[i].length - 1);
		Arrays.sort(partitionSizes);
		//System.out.println(Arrays.toString(partitionSizes));
		br.close();
		return nodePartitions;	
	}

	static int[] getNodePartitions(int[][] graph, int numberOfPartitions) throws Exception
	{
		int numberOfNodes = graph.length;
		final int[] partitionSizes = new int[numberOfPartitions]; 
		Comparator<Integer> partitionSizeComparator = new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return partitionSizes[o1] - partitionSizes[o2];
			}
		};
		PriorityQueue<Integer> smallestPartitions = new PriorityQueue<>(numberOfPartitions, partitionSizeComparator);
		for (int i = 0; i < numberOfPartitions; i++)
			smallestPartitions.add(i);
		
		if (numberOfNodes == 0) return new int[0];
		int freq = (numberOfNodes - (numberOfPartitions - 1)) / numberOfPartitions + 1;

		int[] nodePartitions = new int[numberOfNodes];
		for (int n = 0; n < numberOfNodes; n++)
			nodePartitions[n] = n % numberOfPartitions;
		/*for (int n = 0; n < numberOfNodes; n++)
		{
			int smallestPartition = smallestPartitions.poll();
			nodePartitions[n] = smallestPartition;
			partitionSizes[smallestPartition] += graph[n].length - 1;
			smallestPartitions.add(smallestPartition);
		}*/
		for (int i = 0; i < nodePartitions.length; i++)
			partitionSizes[nodePartitions[i]] += (graph[i].length - 1);
			//partitionSizes[nodePartitions[i]] ++;
		Arrays.sort(partitionSizes);
		//System.out.println(Arrays.toString(partitionSizes));
		return nodePartitions;	
	}

	static int[][] getGraph(String filename, int numberOfNodes) throws Exception
	{
		numberOfNodes = 1300000000;
		int[][] graph = new int[numberOfNodes][];
		int[] numberOfNieghbors = new int[numberOfNodes];
		double sampleRate = SAMPLE_RATE;
		boolean[] sampled = new boolean[1900000000];

		File file = new File(filename);
		long lastPos = 0;

		FileInputStream fis = new FileInputStream(file);
		
		System.out.println("Read entire file into memory");
		String line;
		BufferedReader br = new BufferedReader(new InputStreamReader(fis), Integer.MAX_VALUE / 2);
		Random r = new Random();
		int counter = 0;
		int nEdges = 0;
		while ((line = br.readLine()) != null)
		{
			if (!line.startsWith("#"))
			{
				if (r.nextDouble() < SAMPLE_RATE)
				{
					sampled[counter] = true;
					String[] split = line.split("\\s");
					int from = Integer.parseInt(split[0]);
					int to = Integer.parseInt(split[1]);
					numberOfNieghbors[from]++;
					numberOfNieghbors[to]++;
					nEdges++;
				}
				counter++;
			}
		}
		br.close();
		fis.close();
		System.out.println("Sampled " + nEdges + " edges.");

		for (int i = 0; i < numberOfNodes; i++)
		{	
			int numNeighbors_i = numberOfNieghbors[i];
			if (numNeighbors_i != 0) graph[i] = new int[numNeighbors_i + 1];
		}
		System.out.println("Created graph array");
		int[] currentIndex = new int[numberOfNodes];
		fis = new FileInputStream(file);

		System.out.println("Read entire file into memory");
		br = new BufferedReader(new InputStreamReader(fis), Integer.MAX_VALUE / 2);
		counter = 0;
		while ((line = br.readLine()) != null)
		{
			if (!line.startsWith("#"))
			{
				if (sampled[counter])
				{
					String[] split = line.split("\\s");
					int from = Integer.parseInt(split[0]);
					int to = Integer.parseInt(split[1]);
					graph[from][0] = from;
					graph[from][currentIndex[from] + 1] = to;
					currentIndex[from]++;
					graph[to][0] = to;
					graph[to][currentIndex[to] + 1] = from;
					currentIndex[to]++;
				}
				counter++;
			}
		}
		br.close();
		fis.close();
		System.out.println("Extracted graph info");

		return graph;
	}
	
	static int[][][] getGraphPartitions(int[][] graph, int[] vertexPartitions, int numberOfPartitions)
	{
		int numberOfNodes = graph.length;
		//System.out.println(numberOfNodes);
		//System.out.println(Arrays.toString(vertexPartitions));
		//if (numberOfNodes == 0) return new int[0][0][0];
		int maxNodeId = graph[numberOfNodes - 1][0];
		int[] vertexPartitionMap = new int[maxNodeId + 1];
		for (int i = 0; i < numberOfNodes; i++)
			vertexPartitionMap[graph[i][0]] = vertexPartitions[i];
		int[] partitionSizes = new int[numberOfPartitions];
		for (int i = 0; i < numberOfNodes; i++)
			partitionSizes[vertexPartitions[i]]++;
		int[][][] partitions = new int[numberOfPartitions][][];
		for (int i = 0; i < numberOfPartitions; i++)
			partitions[i] = new int[partitionSizes[i]][];
		int[] currentPartitionIndexes = new int[numberOfPartitions];
		for (int i = 0; i < numberOfNodes; i++)
		{
			int currentPartition = vertexPartitions[i];
			Set<Integer> neighborsInPartion = new HashSet<Integer>(); 
			for (int j = 1; j < graph[i].length; j++)
			{
				int n = graph[i][j];
				if (currentPartition == vertexPartitionMap[n]) neighborsInPartion.add(n);
			}
			int currentPartitionIndex = currentPartitionIndexes[currentPartition];
			partitions[currentPartition][currentPartitionIndex] = new int[neighborsInPartion.size() + 1];
			partitions[currentPartition][currentPartitionIndex][0] = graph[i][0];
			int j = 1;
			for (int e : neighborsInPartion)
				partitions[currentPartition][currentPartitionIndex][j++] = e;
			currentPartitionIndexes[currentPartition]++;
		}
		return partitions;
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

	static class SuperVertexId
	{
		int partitionId;
		int vertexId;
		
		public SuperVertexId(int level1PartitionId, int level2PartitionId,
				int level3PartitionId, int numLevel2Partitioning, int numLevel3Partitioning) {
			super();
			partitionId = level1PartitionId * numLevel2Partitioning + level2PartitionId;
			vertexId = level1PartitionId * numLevel2Partitioning * numLevel3Partitioning + level2PartitionId * numLevel3Partitioning + level3PartitionId;
			//ni * numLevel2Partitioning + nj
			//ni * (numLevel2Partitioning * numLevel3Partitioning) + (nj * numLevel3Partitioning) + nk
		}		

		public int getPartitionId() {
			return partitionId;
		}

		public int getVertexId() {
			return vertexId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + partitionId;
			result = prime * result + vertexId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SuperVertexId other = (SuperVertexId) obj;
			if (partitionId != other.partitionId)
				return false;
			if (vertexId != other.vertexId)
				return false;
			return true;
		}



		int[] toArray()
		{
			return new int[]{partitionId, vertexId};
		}
		
	}

}
