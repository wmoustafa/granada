package io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collection;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class SnapToMetisFormat {

    static final String INPUT_FILE = "input/smallworld50m.txt";
    static final String OUTPUT_FILE = "output/smallworld50m_metis.txt";


	public static void main(String[] args)
	{
		try {
			PrintWriter out = new PrintWriter(OUTPUT_FILE);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(INPUT_FILE)));
			Multimap<Long, Long> graph = TreeMultimap.create();
			String line;

			int edges = 0;
			while ((line = br.readLine()) != null)
			{
				long from = Long.parseLong(line.split("\t")[0]);
				long to = Long.parseLong(line.split("\t")[1]);
				graph.put(from, to);
				graph.put(to, from);
				edges++;
			}
			
			out.println(graph.keySet().size() + " " + edges);
			for (Long v : graph.keySet())
			{
				Collection<Long> neighbors = graph.get(v);
				if (neighbors.isEmpty()); //System.out.println(v);
				StringBuilder out_line = new StringBuilder();
				for (Long n : neighbors)
					out_line.append((n + 1) + " ");
				out_line.deleteCharAt(out_line.length() - 1);
				out.println(out_line.toString());
			}
			out.flush();
			out.close();
			br.close();	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
