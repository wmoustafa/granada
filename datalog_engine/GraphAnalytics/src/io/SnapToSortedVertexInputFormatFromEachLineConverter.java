package io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.zookeeper.Op.Create;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

public class SnapToSortedVertexInputFormatFromEachLineConverter {

	static int NUMBER_OF_HASH_FUNCTIONS = 2;
	static int NUMBER_OF_PARTITIONS = 200;
	static int[] hash_function_xor_number = new int[NUMBER_OF_HASH_FUNCTIONS];

	public static void main(String[] args)
	{
		try {
			long mincut = Integer.MAX_VALUE;
			for (int trials = 0; trials < 200; trials++)
			{
			Random rand = new Random();
			for (int i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++) hash_function_xor_number[i] = rand.nextInt();
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
				vertexPartition.put(v, partitionNumber);
				vertexNumber++;
				if (vertexNumber == numberOfVertices/NUMBER_OF_PARTITIONS) { vertexNumber = 0; partitionNumber++; }
			}
			
			long crossingEdges = PartitioningExperiment.getCommunicationVolume(graph, vertexPartition);
			if (mincut > crossingEdges) mincut = crossingEdges;
			//for (String p : partitions.keySet())
				////System.out.println(partitions.get(p).size());
			//System.out.println("**"+crossingEdges);
			br.close();
			}

			//System.out.println(mincut);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
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
	
	static long getNumberOfCrossEdges(Multimap<Long, Long> graph, Map<Long, Integer> vertexPartition)
	{
		long crossEdges = 0;
		for (Entry<Long, Long> e : graph.entries())
		{
			long v = e.getKey();
			long n = e.getValue();
			if (!vertexPartition.get(v).equals(vertexPartition.get(n))) crossEdges++;
		}
		return crossEdges/2;
	}


}
