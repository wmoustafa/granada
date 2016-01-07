package main;

import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.GiraphRunner;

import algebra.Program;
import algebra.Rule;
import parser.DatalogVariable;
import parser.Operation;
import parser.Parser;
import parser.StringConst;
import schema.Table;
import schema.Database;
import schema.Tuple;

public class Main {

	/**
	 * @param args
	 */
	public static Program program;
	
	public static void main(String[] args) {
		try 
		{
			//populateDB();
			long t1=System.currentTimeMillis();
			
			String giraphArgsString = "-libjars /Users/walaa/Documents/workspace/WordCount/lib/wordcnt.jar DatalogComputation "
					+ "-vip hdfs://yoda40:54310/user/walaa/input/FamilyGraph.txt -vif  RelationalVertexInputFormatFromEachLine "
					+ "-vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat "
					+ "-op hdfs://yoda40:54310/user/walaa/output/shortestpaths8 -w 1 -ca giraph.SplitMasterWorker=false";
			String[] giraphArgs = giraphArgsString.split("\\s+");
			GiraphRunner.main(giraphArgs);
			long t2=System.currentTimeMillis();
			
			//System.out.println((t2-t1) +" ms");
			
			
			
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}

	}
	
	private static void populateDB()
	{
		Table nodes = new Table(new Class[] {String.class, String.class, String.class}, new int[]{0});
		Table edges = new Table(new Class[] {String.class, String.class}, new int[]{0});
//		nodes.addTuple(new Tuple(new Object[]{"W", "Walaa", "M"}));
//		nodes.addTuple(new Tuple(new Object[]{"E", "Ethar", "F"}));
//		nodes.addTuple(new Tuple(new Object[]{"A", "Ayman", "M"}));
//		nodes.addTuple(new Tuple(new Object[]{"B", "Berenice", "F"}));
//		nodes.addTuple(new Tuple(new Object[]{"S", "Sami", "M"}));
//		
//		edges.addTuple(new Tuple(new Object[]{"W", "E"}));
//		edges.addTuple(new Tuple(new Object[]{"W", "A"}));
//		edges.addTuple(new Tuple(new Object[]{"W", "S"}));
//		edges.addTuple(new Tuple(new Object[]{"E", "A"}));
//		edges.addTuple(new Tuple(new Object[]{"A", "B"}));
//		edges.addTuple(new Tuple(new Object[]{"E", "W"}));
//		edges.addTuple(new Tuple(new Object[]{"A", "W"}));
//		edges.addTuple(new Tuple(new Object[]{"S", "W"}));
//		edges.addTuple(new Tuple(new Object[]{"A", "E"}));
//		edges.addTuple(new Tuple(new Object[]{"B", "A"}));
		
		//database.addDataTable("nodes", nodes);
		//database.addDataTable("edges", edges);
	}

}
