package io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.gson.Gson;

public class SnapToPartitionedGiraphLocal {

	/*static final String SNAP_FILENAME = "files/smallworld.txt";
	static final int NUMBER_OF_NODES = 50000000;
	static final int NUM_LEVEL1_PARTITIONS = 100;
	static final int NUM_LEVEL2_PARTITIONS = 100;
	static final int NUM_LEVEL3_PARTITIONS = 100;
	static final int NUMBER_OF_LABELS = 50;
	static final String OUTPUT_FILE = "files/smallworld.txt.hdfs";
	static final String TMP_DIR = "~/tmp/metis";*/
	
	static final String SNAP_FILENAME = "graph_files/smallworld100.txt";
	static final int NUMBER_OF_NODES = 100;
	static final int NUM_LEVEL1_PARTITIONS = 2;
	static final int NUM_LEVEL2_PARTITIONS = 2;
	static final int NUM_LEVEL3_PARTITIONS = 10;
	static final int NUMBER_OF_LABELS = 5;
	static final String OUTPUT_FILE = "files/smallworld100.txt.hdfs";
	static final String TMP_DIR = "files/tmp/metis";


	public static void main(String[] args) throws Exception
	{
		File tmpDir = new File(TMP_DIR, UUID.randomUUID().toString());
		tmpDir.mkdirs();

		int[][] threeLevelPartitioning = new int[NUMBER_OF_NODES][3];

		int[][] graph = getGraph(SNAP_FILENAME, NUMBER_OF_NODES);
		int[] level1VertexPartitions = getNodePartitions(graph, NUM_LEVEL1_PARTITIONS, tmpDir);
		int[][][] level1Graphs = getGraphPartitions(graph, level1VertexPartitions, NUM_LEVEL1_PARTITIONS);

		assignThreeLevelPartitioning(graph, level1VertexPartitions, threeLevelPartitioning, 0);
		for (int i = 0; i < NUM_LEVEL1_PARTITIONS; i++)
		{
			int[] level2VertexPartitions = getNodePartitions(level1Graphs[i], NUM_LEVEL2_PARTITIONS, tmpDir);
			int[][][] level2Graphs = getGraphPartitions(level1Graphs[i], level1VertexPartitions, NUM_LEVEL2_PARTITIONS);

			assignThreeLevelPartitioning(level1Graphs[i], level2VertexPartitions, threeLevelPartitioning, 1);

			for (int j = 0; j < NUM_LEVEL2_PARTITIONS; j++)
			{
				int[] level3VertexPartitions = getNodePartitions(level2Graphs[j], NUM_LEVEL3_PARTITIONS, tmpDir);
				//int[][][] level3GraphPartitions = getGraphPartitions(level2Graphs[i], level2VertexPartitions, NUM_LEVEL2_PARTITIONS);

				assignThreeLevelPartitioning(level2Graphs[j], level3VertexPartitions, threeLevelPartitioning, 2);
			}
		}
		int[][][][] verticesInEachPartition = getVerticesInEachPartition(graph, threeLevelPartitioning, NUM_LEVEL1_PARTITIONS, NUM_LEVEL2_PARTITIONS, NUM_LEVEL3_PARTITIONS);
		writeHdfsFile(graph, OUTPUT_FILE, NUMBER_OF_LABELS, NUMBER_OF_NODES, verticesInEachPartition, threeLevelPartitioning, NUM_LEVEL1_PARTITIONS, NUM_LEVEL2_PARTITIONS, NUM_LEVEL3_PARTITIONS);

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

	static void writeHdfsFile(int[][] graph, String outputFile, int numberOfLabels, int numberOfNodes, int[][][][] verticesInEachPartition, int[][] threeLevelPartitioning, int numLevel1Partitioning, int numLevel2Partitioning, int numLevel3Partitioning) throws Exception
	{
		double zipfDistributionDenominator = 0;
		for (int k = 1; k <= numberOfLabels; k++) zipfDistributionDenominator += 1.0 / k;
		double[] zipfDistribution = new double[numberOfLabels + 1];
		zipfDistribution[0] = 0;
		for (int k = 1; k <= numberOfLabels; k++) zipfDistribution[k] = zipfDistribution[k - 1] + (1.0 / k) / zipfDistributionDenominator;

		PrintWriter out = new PrintWriter(outputFile);
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
					for (int n : verticesInEachPartition[i][j][k])
					{
						Set<Integer> outEdges = new HashSet<Integer>();
						Set<Integer> inEdges = new HashSet<Integer>();
						for (int e = 1; e < graph[n].length; e++)
						{
							int neighborId = graph[n][e];
							if (((n ^ neighborId) & 0x01) == 1)
								if (n > neighborId) outEdges.add(neighborId); else inEdges.add(neighborId);
							else 
								if (n < neighborId) outEdges.add(neighborId); else inEdges.add(neighborId);
							//if (Math.random() < thisNodesOutEdgeProb) outEdges.add(neighborId); else inEdges.add(neighborId);
							int ni = threeLevelPartitioning[neighborId][0];
							int nj = threeLevelPartitioning[neighborId][1];
							int nk = threeLevelPartitioning[neighborId][2];
							SuperVertexId neighborSuperVertexId = new SuperVertexId(ni, nj, nk, numLevel2Partitioning, numLevel3Partitioning);
							if (!superVertexId.equals(neighborSuperVertexId)) 
								superNeighbors.add(neighborSuperVertexId);
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
						vertexArray[1] = outEdges.toArray();
						vertexArray[2] = inEdges.toArray();
						//int i
						//for (List<Integer> neighborSuperVertexInfo : neighborsSuperVertices)
						
						
						superVertexData[c++] = vertexArray;
					}
					
					superVertexArray[1] = superVertexData;
					
					Object[] superVertexNeighborsArray = new Object[superNeighbors.size()];
					int e = 0;
					for (SuperVertexId level3Neighbor : superNeighbors)
						superVertexNeighborsArray[e++] = level3Neighbor.toArray();
					superVertexArray[2] = superVertexNeighborsArray;
					superVertexArray[3] = superNeighborsDetails;
					//System.out.println(new Gson().toJson(superVertexArray));
					out.println(new Gson().toJson(superVertexArray));
				}

		out.flush();
		out.close();		
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
		int numberOfNodes = graph.length;
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
		ProcessBuilder b3 = new ProcessBuilder("/usr/local/bin/gpmetis", absoluteOutputFilename, String.valueOf(numberOfPartitions));
		Process p3 = b3.start();
		p3.waitFor();

		String metisPartitionsFile = absoluteOutputFilename + ".part." + numberOfPartitions;
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(metisPartitionsFile)));
		String line3;
		int[] nodePartitions = new int[numberOfNodes];
		int n = 0;
		while ((line3 = br.readLine()) != null)
		{
			int partition = Integer.parseInt(line3);
			nodePartitions[n++] = partition;
		}
		br.close();
		return nodePartitions;	
	}

	static int[][] getGraph(String filename, int numberOfNodes) throws Exception
	{
		int[][] graph = new int[numberOfNodes][];
		int[] numberOfNieghbors = new int[numberOfNodes];
		String line;
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
		while ((line = br.readLine()) != null)
		{
			if (!line.startsWith("#"))
			{
				String[] split = line.split("\t");
				int from = Integer.parseInt(split[0]);
				int to = Integer.parseInt(split[1]);
				numberOfNieghbors[from]++;
				numberOfNieghbors[to]++;				
			}
		}
		br.close();
		for (int i = 0; i < numberOfNodes; i++)
			graph[i] = new int[numberOfNieghbors[i] + 1];
		int[] currentIndex = new int[numberOfNodes];
		br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
		while ((line = br.readLine()) != null)
		{
			if (!line.startsWith("#"))
			{
				String[] split = line.split("\t");
				int from = Integer.parseInt(split[0]);
				int to = Integer.parseInt(split[1]);
				graph[from][0] = from;
				graph[from][currentIndex[from] + 1] = to;
				currentIndex[from]++;
				graph[to][0] = to;
				graph[to][currentIndex[to] + 1] = from;
				currentIndex[to]++;
			}
		}
		br.close();

		return graph;
	}
	
	static int[][][] getGraphPartitions(int[][] graph, int[] vertexPartitions, int numberOfPartitions)
	{
		int numberOfNodes = graph.length;
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
