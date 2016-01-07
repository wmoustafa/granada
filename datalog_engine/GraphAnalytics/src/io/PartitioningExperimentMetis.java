package io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.Op.Create;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

public class PartitioningExperimentMetis {

	static int NUMBER_OF_PARTITIONS = 200;
	static int NUMBER_OF_HASH_FUNCTIONS = 5;
	static int NUMBER_OF_BANDS = 10;
	static boolean USE_JACCARD = true;
	static boolean START_FROM_RANDOM_PARTITIONS = false;
	static double STATISTICAL_PARTITION_ASSIGNMENT_PROB = 0.5;
	static int[] hash_function_xor_number = new int[NUMBER_OF_HASH_FUNCTIONS];

	public static void main(String[] args)
	{
		try {
			Random rand = new Random();
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("files/smallworld10k_1.txt")));
			Multimap<Long, Long> graph = HashMultimap.create();
			Map<Long, String> signatures = new HashMap<Long, String>();
			String line;

			while ((line = br.readLine()) != null)
			{
				long from = Long.parseLong(line.split("\t")[0]);
				long to = Long.parseLong(line.split("\t")[1]);
				graph.put(from, to);
				graph.put(to, from);
			}
			br.close();

			Map<Long, Integer> vertexMigrationPartition = new HashMap<Long, Integer>();
			long numberOfVertices = graph.keySet().size();
			Multimap<String, Long> partitions = HashMultimap.create();

			for (long v : graph.keySet())
			{
				signatures.put(v, getSignature(graph, v));
				partitions.put(getSignature(graph, v), v);
			}
			
			Map<Long, String> sortedSignatures = MapUtil.sortByValue(signatures);
			
			Map<Long, Integer> vertexPartition = new HashMap<Long, Integer>();
			int partitionNumber = 0;
			int vertexNumber = 0;
			for (long v : sortedSignatures.keySet())
			{
				if (START_FROM_RANDOM_PARTITIONS) vertexPartition.put(v, rand.nextInt(NUMBER_OF_PARTITIONS)); else vertexPartition.put(v, partitionNumber);
				vertexNumber++;
				if (vertexNumber == numberOfVertices/NUMBER_OF_PARTITIONS) { vertexNumber = 0; partitionNumber++; }
			}

			//System.out.println(getCommunicationVolume(graph, vertexPartition));

			for (int iterations = 0; iterations < 100; iterations++)
			{
				int[] partitionSizes = new int[NUMBER_OF_PARTITIONS];
				for (long v : graph.keySet())
					partitionSizes[vertexPartition.get(v)]++;
				////System.out.println(Arrays.toString(partitionSizes));

				int[][] swappingCounts = new int[NUMBER_OF_PARTITIONS][NUMBER_OF_PARTITIONS];
				int[][] actuallySwapped = new int[NUMBER_OF_PARTITIONS][NUMBER_OF_PARTITIONS];
				double[][] swappingProb = new double[NUMBER_OF_PARTITIONS][NUMBER_OF_PARTITIONS];
				for (long v : graph.keySet())
				{
					double[] migrationProb = getMigrationProb(graph, v, vertexPartition);
					int migrationPartition = getMigrationPartitionMax(migrationProb);
					vertexMigrationPartition.put(v, migrationPartition);
					swappingCounts[vertexPartition.get(v)][migrationPartition]++;
				}
				
				/*//System.out.println("*************SwappingCount***************");
				for (int i = 0; i < NUMBER_OF_PARTITIONS; i++)
					//System.out.println(Arrays.toString(swappingCounts[i]));*/
				for (int i = 0; i < NUMBER_OF_PARTITIONS; i++)
					for (int j = i; j < NUMBER_OF_PARTITIONS; j++)
					{
						int numberSwapping = swappingCounts[i][j] < swappingCounts[j][i]? swappingCounts[i][j] : swappingCounts[j][i];
						if (numberSwapping==0) swappingProb[i][j] = 0; else swappingProb[i][j] = (double)numberSwapping/swappingCounts[i][j];
						if (numberSwapping==0) swappingProb[j][i] = 0; else swappingProb[j][i] = (double)numberSwapping/swappingCounts[j][i];
					}
				//for (int i = 0; i < NUMBER_OF_PARTITIONS; i++)
				//	//System.out.println(Arrays.toString(swappingProb[i]));
				for (long v : graph.keySet())
				{
					int from_partition = vertexPartition.get(v);
					int to_partition = vertexMigrationPartition.get(v);
					double coin = Math.random();
					if (coin < swappingProb[from_partition][to_partition]) { vertexPartition.put(v, to_partition); actuallySwapped[from_partition][to_partition]++; }
				}
				/*//System.out.println("*************ActuallySwapped***************");
				for (int i = 0; i < NUMBER_OF_PARTITIONS; i++)
					//System.out.println(Arrays.toString(actuallySwapped[i]));*/
				//System.out.println(getCommunicationVolume(graph, vertexPartition));
			}
			//System.out.println(getEdgeCut(graph, vertexPartition));
			
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	static int getPartition(Multimap<Long, Long> graph, long v) {
		int min_h = Integer.MAX_VALUE;
		Set<Long> subgraph = new HashSet<>();
		subgraph.add(v);
		subgraph.addAll(graph.get(v));
		for (Long n : subgraph)
		{
			int h = n.hashCode();
			if (h < min_h) min_h = h;
		}
		//return new Random().nextInt(NUMBER_OF_PARTITIONS);
		return new Long(v).hashCode() % NUMBER_OF_PARTITIONS;
		//return min_h/NUMBER_OF_PARTITIONS;
	}
	
	static double[] getMigrationProb(Multimap<Long, Long> graph, long v, Map<Long, Integer> vertexPartition)
	{
		double[] migrationProb = new double[NUMBER_OF_PARTITIONS];
		double sum = 0;
		for (Long n : graph.get(v))
		{
			double j;
			if (USE_JACCARD) j = jaccard(graph, v, n); else j = 1;
			migrationProb[vertexPartition.get(n)] += j;
			sum += j;
		}
		for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) migrationProb[i] /= sum;
		return migrationProb;
	}
	
	static double jaccard(Multimap<Long, Long> graph, long v, long n)
	{
		Set<Long> num = new HashSet<Long>();
		Set<Long> den = new HashSet<Long>();
		num.addAll(graph.get(v));
		num.retainAll(graph.get(n));
		den.addAll(graph.get(v));
		den.addAll(graph.get(n));
		return (double)num.size()/den.size();
	}
	static int getMigrationPartition(double[] migrationProb)
	{
		double migrationCoin = Math.random();
		double comulativeProb = 0;
		for (int i = 0; i < NUMBER_OF_PARTITIONS; i++)
		{
			double p = migrationProb[i];
			if (migrationCoin > comulativeProb && migrationCoin <= p + comulativeProb) return i;
			comulativeProb += p;
		}
		return -1;
	}
	
	static int getMigrationPartitionMax(double[] migrationProb)
	{
		if (Math.random() < STATISTICAL_PARTITION_ASSIGNMENT_PROB) return getMigrationPartition(migrationProb);
		double maxProb = 0;
		int maxPartition = -1;
		for (int i = 0; i < NUMBER_OF_PARTITIONS; i++)
		{
			double p = migrationProb[i];
			if (p > maxProb) { maxProb = p; maxPartition = i; }
		}
		return maxPartition;
	}

	static long getCommunicationVolume(Multimap<Long, Long> graph, Map<Long, Integer> vertexPartition)
	{
		long crossEdges = 0;
		for (Long v : graph.keySet())
		{
			Set<Integer> otherPartitions = new HashSet<Integer>(); 
			for (Long n : graph.get(v))
			{
				if (!vertexPartition.get(v).equals(vertexPartition.get(n))) otherPartitions.add(vertexPartition.get(n));
			}
			crossEdges += otherPartitions.size();
		}
		return crossEdges;
	}

	static long getEdgeCut(Multimap<Long, Long> graph, Map<Long, Integer> vertexPartition)
	{
		long crossEdges = 0;
		for (Long v : graph.keySet())
		{
			for (Long n : graph.get(v))
				if (!vertexPartition.get(v).equals(vertexPartition.get(n))) crossEdges ++;
		}
		return crossEdges/2;
	}

	static String getSignature(Multimap<Long, Long> graph, long v) {
		int[] min_h = new int[NUMBER_OF_HASH_FUNCTIONS];
		for (int i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++) min_h[i] = Integer.MAX_VALUE;
		Set<Long> subgraph = new HashSet<>();
		subgraph.add(v);
		subgraph.addAll(graph.get(v));
		for (Long n : subgraph)
		{
			int h = n.hashCode();
			for (int i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++)
			{
				int h_i = ((h >>> i) | (h << (Integer.SIZE - i))) ^ hash_function_xor_number[i];
				if (h_i < min_h[i]) min_h[i] = h_i;
			}
		}
		ByteBuffer b = ByteBuffer.allocate(NUMBER_OF_HASH_FUNCTIONS * 4);
		for (int h : min_h)
			b.putInt(h);
		String s = new String(b.array());
		return s;		
	}


}
