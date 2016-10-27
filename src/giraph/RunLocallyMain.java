package giraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.json.JSONArray;
import org.json.JSONException;

import algebra.Program;
import algebra.RelationalType;
import algebra.Rule;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import parser.ParseException;
import parser.Parser;
import schema.Database;
import schema.Metadata;
import schema.Table;
import schema.Tuple;
import utils.AggregationFunctionType;

public class RunLocallyMain {

	 // graph of super-vertices
	public static Map<SuperVertexId,Database> input_graph = new HashMap<SuperVertexId, Database>();
	static DatalogDependencyGraph g;
	static List<Rule> rulesToProcess;
	static Program rewrittenProgram;
	static Map<String,Boolean> changed_aggregate = new HashMap<>();
	static Metadata metadata;
	static String program_name = "wcc";
	static Map<SuperVertexId, List<Database>> send_messages = new HashMap<>();
	static String path = "/Users/papavas/Documents/UCSD/research/nec_git_repository/granada/datasets/";
	
	public static void main(String args[]) throws IOException, InterruptedException, JSONException, ParseException
	{
		String input_file = new String(path+"small_20.1.datalog.txt"); //FIXME 
		String dl_program = new String(path+"wcc_new.txt"); //FIXME
		program_name = "wcc";
		
		loadGraph(input_file, input_graph);
		preComputation(dl_program);
		
		for(int superstep=0; superstep<30; superstep++)
		{
			System.out.println("Now at superstep " + superstep);
			preSuperstep(superstep);
			for(SuperVertexId id: input_graph.keySet())
			{
				//Evaluate rule per super-vertex
				compute(input_graph.get(id), send_messages.get(id));
			}
		}
		
	}
	
	
	private static void compute(Database inputDatabase, List<Database> messages)
	{
		Int2ObjectOpenHashMap<SuperVertexId> neighbors = new Int2ObjectOpenHashMap<SuperVertexId>();
		//System.out.println("Vertex value = " + vertex.getValue());
		Database relationalDatabase = new Database();

		//Vicky: Combine messages from all neighbors into one message. Combine databases in per-table basis
		Database messagesDb = new Database();
		
		if(messages != null){
			for (Database message : messages){
				messagesDb.combine2(message);				
			}			
		}
		//System.out.println("Message database after combining=" + messagesDb);
		//assert(!messagesDb.isEmpty());		
		Set<String> changedTables = new HashSet<>();
		Set<String> changed = inputDatabase.refresh(messagesDb);
		
		for (Rule rule : rulesToProcess)
		{
			Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase, metadata);
			
			//TODO for PageRank improvement to remove intermediate results
			if (program_name.equals("page_rank")) {   
				inputDatabase.removeDataTable(rule.getHead().getName());
				inputDatabase.removeDataTable(rule.getHead().getName() + "_full");
			}
			
		
			if (rule.getRelationalType() == RelationalType.NOT_RELATIONAL)
			{
				inputDatabase.refresh(outputDatabase);
			}
			else
			{
				changed = relationalDatabase.combine2(outputDatabase);
				changedTables.addAll(changed);
			}
		}

		
		for (String table : changedTables)
			changed_aggregate.put(table, new Boolean(true));

		if (!relationalDatabase.isEmpty())
		{
			Map<SuperVertexId, Database> superVertexIdToDatabase = null;
	
				superVertexIdToDatabase = relationalDatabase.
				getDatabasesForEverySuperVertexEdgeBased(inputDatabase, neighbors);
			
			for (Entry<SuperVertexId, Database> entry : superVertexIdToDatabase.entrySet())
			{
				SuperVertexId neighborId = entry.getKey();
				Database neighborDb = entry.getValue();
				if(!send_messages.containsKey(neighborId))
				{
					send_messages.put(neighborId, new ArrayList<Database>());
				}
				send_messages.get(neighborId).add(neighborDb);
			}
		}
		
	}
	
	
	
	private static void preSuperstep(int superstep)
	{
		//Prepare for computation
		if (superstep == 0)
		{
			rulesToProcess = g.getFirstToProcess();
		}
		else
		{
			for (Rule rule : rulesToProcess)
				changed_aggregate.put(rule.getHead().getName(), true);

//			System.out.println("changed" + changed_aggregate);
			rulesToProcess = g.getNextToProcess(changed_aggregate);
//			System.out.println("predicatesToProcess " + rulesToProcess);
		}
		System.out.println("-----> Now going to process: " +rulesToProcess);
		////System.out.println("Free memory: " + Runtime.getRuntime().freeMemory()/1024/1024);
		for (Rule rule : rulesToProcess) {
			System.out.println("Evaluating rule " + rule + " with plan ");
			rule.generateEvaluationPlan(null,metadata);
			rule.getEvaluationPlan().print();
		}
		changed_aggregate.clear();
	}
	
	private static void preComputation(String dl_program) throws FileNotFoundException, ParseException
	{
		FileInputStream fi = new FileInputStream(new File(dl_program));

		Parser parser = new Parser(fi);
		Program program = parser.program();

		for (Rule rule : program.getRules())
		{
			System.out.println(rule + " rel:" + rule.getRelationalType() + " agg:" + rule.isAggregate());
		}
		rewrittenProgram = program.rewrite(true, true);
		System.out.println(" REWRITTEN PROGRAM:");
		for (Rule rule : rewrittenProgram.getRules())
		{
			System.out.println(rule + " rel:" + rule.getRelationalType() + " agg:" + rule.isAggregate());
		}
		
		g = new DatalogDependencyGraph(rewrittenProgram);
		g.setRecursivePredicatesForRules();
		
		
		metadata = new Metadata();
		int[] vertexKeyFields = new int[]{0};
		Class[] vertexFieldTypes = new Class[]{Integer.class, Integer.class};

		int[] edgeKeyFields = new int[]{0};
		Class[] edgeFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
		
		int[] incomingNeighborsKeyFields = new int[]{0};
		Class[] incomingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};

		int[] outgoingNeighborsKeyFields = new int[]{0};
		Class[] outgoingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
		
		metadata.setMetadata("vertices", vertexKeyFields, vertexFieldTypes);
		metadata.setMetadata("edges", edgeKeyFields, edgeFieldTypes);
		metadata.setMetadata("incomingNeighbors", incomingNeighborsKeyFields, incomingNeighborsFieldTypes);
		metadata.setMetadata("outgoingNeighbors", outgoingNeighborsKeyFields, outgoingNeighborsFieldTypes);
	}

	private static void loadGraph(String input,Map<SuperVertexId,Database> input_graph) throws IOException,InterruptedException, JSONException {
		BufferedReader bfi = new BufferedReader(new FileReader(new File(input)));
		
		// each line of input file contains the data and id of one super-vertex
		String line=null;
		while((line = bfi.readLine()) != null)
		{
			JSONArray in_a = new JSONArray(line.toString());
			JSONArray superVertexId = in_a.getJSONArray(0);
			SuperVertexId s_id =  new SuperVertexId((short)(superVertexId.getInt(0)), superVertexId.getInt(1));
			Database data = readSuperVertexData(in_a);
			input_graph.put(s_id, data);
		}
		bfi.close();
	}
	
	private static Database readSuperVertexData(JSONArray input) throws JSONException, IOException
	{
		
			Metadata metadata = new Metadata();
			
			JSONArray jsonSuperVertexValues = input.getJSONArray(1);
			
			int[] vertexKeyFields = new int[]{0};
			//Class[] vertexFieldTypes = new Class[]{Integer.class, String.class, String.class};
			Class[] vertexFieldTypes = new Class[]{Integer.class, Integer.class};
			Table vertexTable = new Table(vertexFieldTypes, vertexKeyFields, jsonSuperVertexValues.length());
			
			int[] edgeKeyFields = new int[]{0};
			Class[] edgeFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
//			Table edgesTable = new Table(edgeFieldTypes, edgeKeyFields, nEdges);

			int[] outgoingNeighborsKeyFields = new int[]{0};
			Class[] outgoingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
			Table outgoingNeighborsTable = new Table(outgoingNeighborsFieldTypes, outgoingNeighborsKeyFields, jsonSuperVertexValues.length());

			int[] incomingNeighborsKeyFields = new int[]{0};
			Class[] incomingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
//			Table incomingNeighborsTable = new Table(incomingNeighborsFieldTypes, incomingNeighborsKeyFields, jsonSuperVertexValues.length());

			int[] messagesKeyFields = new int[]{0};
			Class[] messagesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
			Table messagesTable = new Table(messagesFieldTypes, messagesKeyFields);
			messagesTable.setAggregationFunctionType(AggregationFunctionType.SUM);;

			JSONArray jsonNeighborSuperVertices = input.getJSONArray(3);

			int[] neighborSuperVerticesKeyFields = new int[]{0};
			//Class[] vertexFieldTypes = new Class[]{Integer.class, String.class, String.class};
			Class[] neighborSuperVerticesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
			Table neighborSuperVerticesTable = new Table(neighborSuperVerticesFieldTypes, neighborSuperVerticesKeyFields, jsonNeighborSuperVertices.length());
			

			long t1 = System.currentTimeMillis();
			for (int i = 0; i < jsonSuperVertexValues.length(); i++)
			{
				JSONArray jsonVertexValues = jsonSuperVertexValues.getJSONArray(i);
				JSONArray jsonVertexTuple = jsonVertexValues.getJSONArray(0);
				JSONArray jsonInEdgeTupleArray = jsonVertexValues.getJSONArray(1);
				JSONArray jsonOutEdgeTupleArray = jsonVertexValues.getJSONArray(2);

				int[] vertexTuple = new int[jsonVertexTuple.length()];
				for (int j = 0; j < jsonVertexTuple.length(); j++)
				{
					if (vertexFieldTypes[j] == String.class) 
						throw new RuntimeException("String: Unsupported data type");
					else if (vertexFieldTypes[j] == Integer.class) 
						vertexTuple[j] = jsonVertexTuple.getInt(j);
					else if (vertexFieldTypes[j] == Boolean.class) 
						throw new RuntimeException("Boolean: Unsupported data type");
				}				
				vertexTable.putTuple(vertexTuple);
			

				int[] messagesTuple = new int[3];
				messagesTuple[0] = jsonVertexTuple.getInt(0);
				messagesTuple[1] = 0;
				messagesTuple[2] = 0;
				messagesTable.putTuple(messagesTuple);

				for (int j = 0; j < jsonOutEdgeTupleArray.length(); j++)
				{
					int[] edgeTuple = new int[3];
					edgeTuple[0] = jsonVertexTuple.getInt(0);
					JSONArray edgeEndVertexAndWeight = jsonOutEdgeTupleArray.getJSONArray(j);
					edgeTuple[1] = edgeEndVertexAndWeight.getInt(0);
					edgeTuple[2] = edgeEndVertexAndWeight.getInt(1);
//					edgesTable.putTuple(new Tuple(edgeTuple));
					
					outgoingNeighborsTable.putTuple(edgeTuple);
					edgeEndVertexAndWeight = null;
				}

			}

						
			for (int i = 0; i < jsonNeighborSuperVertices.length(); i++)
			{
				JSONArray jsonNeighborSuperVertexTuple = jsonNeighborSuperVertices.getJSONArray(i);
				int[] neighborSuperVertexTuple = new int[3];
				neighborSuperVertexTuple[0] = jsonNeighborSuperVertexTuple.getInt(0);
				neighborSuperVertexTuple[1] = jsonNeighborSuperVertexTuple.getInt(1);
				neighborSuperVertexTuple[2] = jsonNeighborSuperVertexTuple.getInt(2);
				neighborSuperVerticesTable.putTuple(neighborSuperVertexTuple);
			}


			metadata.setMetadata("vertices", vertexKeyFields, vertexFieldTypes);
			metadata.setMetadata("edges", edgeKeyFields, edgeFieldTypes);
			metadata.setMetadata("incomingNeighbors", incomingNeighborsKeyFields, incomingNeighborsFieldTypes);
			metadata.setMetadata("outgoingNeighbors", outgoingNeighborsKeyFields, outgoingNeighborsFieldTypes);
			
//			System.out.println("Metadata after reading input " + metadata);
			
			Database database = new Database(metadata,-1);
//			StringBuffer sb = new StringBuffer();
//			sb.append("[Empty database object = " + MemoryMeasurer.measureBytes(database) + "].");
			database.addDataTable("vertices", vertexTable);
//			sb.append("[Size of vertices = " + MemoryMeasurer.measureBytes(vertexTable) + "]");
//			sb.append("[database after vertices = " + MemoryMeasurer.measureBytes(database) + "].");
//			database.addDataTable("edges", edgesTable); //TODO Vicky checking if can be removed
//			sb.append("[database after edges = " + MemoryMeasurer.measureBytes(database) + "].");
//			database.addDataTable("incomingNeighbors", incomingNeighborsTable); //TODO Vicky checking if can be removed
//			sb.append("[database after in neigh = " + MemoryMeasurer.measureBytes(database) + "].");
			database.addDataTable("outgoingNeighbors", outgoingNeighborsTable);
//			sb.append("[database after out neigh = " + MemoryMeasurer.measureBytes(database) + "].");
			database.addDataTable("neighborSuperVertices", neighborSuperVerticesTable);
//			sb.append("[database after neighbor super = " + MemoryMeasurer.measureBytes(database) + "].");
			database.addDataTable("messages_full", messagesTable);
//			sb.append("[database after msg table = " + MemoryMeasurer.measureBytes(database) + "].");

			return database;
	}
}
