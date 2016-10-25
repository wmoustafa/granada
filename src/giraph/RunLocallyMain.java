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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import algebra.Program;
import algebra.RelationalType;
import algebra.Rule;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import objectexplorer.MemoryMeasurer;
import objectexplorer.ObjectGraphMeasurer;
import objectexplorer.ObjectGraphMeasurer.Footprint;
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
//		System.out.println("[Size of total graph  = " + MemoryMeasurer.measureBytes(input_graph) + "].");
		preComputation(dl_program);
		
		for(int superstep=0; superstep<10; superstep++)
		{
			System.out.println("############################--> Now at superstep " + superstep);
			preSuperstep(superstep);
			for(SuperVertexId id: input_graph.keySet())
			{
				//Evaluate rule per super-vertex
//				System.out.println("------> Now at super-vertex " + id);
				compute(id, input_graph.get(id), send_messages.get(id));
			}
			Metadata.tuple_counter = 0;
		}
		
	}
	
	private static long usedMemory()
	{
		return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
	}
	
	private static void compute(SuperVertexId id, Database inputDatabase, List<Database> messages)
	{
		
		//Memory measurer

		StringBuilder sb = new StringBuilder();
		
		Int2ObjectOpenHashMap<SuperVertexId> neighbors = new Int2ObjectOpenHashMap<SuperVertexId>();
		Database relationalDatabase = new Database();
		Database messagesDb = new Database();
		
		if(messages != null){
			for (Database message : messages){
				messagesDb.combine2(message);				
			}			
		}
//		sb.append("[Size of incoming messages  = " + MemoryMeasurer.measureBytes(messages) + "].");
//		sb.append("[Size of messagesDB after combine = " + MemoryMeasurer.measureBytes(messagesDb) + "].");
		
		//clear messages 
		send_messages.remove(id);
				
		Set<String> changedTables = new HashSet<>();
		Set<String> changed = inputDatabase.refresh(messagesDb);
//		sb.append("[Size of input database = " + MemoryMeasurer.measureBytes(inputDatabase) + "].");
//		Footprint footprint = ObjectGraphMeasurer.measure(inputDatabase);
//		sb.append("InputDatabase = " +footprint);
		
		for (Rule rule : rulesToProcess)
		{
			Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase, metadata);
//			sb.append("[Size of outputDatabase = " + MemoryMeasurer.measureBytes(outputDatabase) + "].");
			
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
//		sb.append("[Size of inputDatabase after plan evaluation = " + MemoryMeasurer.measureBytes(inputDatabase) + "].");
//		footprint = ObjectGraphMeasurer.measure(inputDatabase);
//		sb.append("InputDatabase = " +footprint);
//		sb.append("[Size of relationalDatabase = " + MemoryMeasurer.measureBytes(relationalDatabase) + "].");
		
		for (String table : changedTables)
			changed_aggregate.put(table, new Boolean(true));

		if (!relationalDatabase.isEmpty())
		{
			Map<SuperVertexId, Database> superVertexIdToDatabase = null;
	
				superVertexIdToDatabase = relationalDatabase.
				getDatabasesForEverySuperVertexEdgeBased(inputDatabase, neighbors);
//				sb.append("[Size of superVertexIdToDatabase = " + MemoryMeasurer.measureBytes(superVertexIdToDatabase) + "].");
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
//		System.out.println(sb.toString());
//		System.out.println("New tuple objects created: " + Metadata.tuple_counter);
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
//		System.out.println("Now going to process: " +rulesToProcess);
		////System.out.println("Free memory: " + Runtime.getRuntime().freeMemory()/1024/1024);
		for (Rule rule : rulesToProcess) {
//			System.out.println("Evaluating rule " + rule + " with plan ");
			rule.generateEvaluationPlan(null,metadata);
//			rule.getEvaluationPlan().print();
		}
		changed_aggregate.clear();
	}
	
	private static void preComputation(String dl_program) throws FileNotFoundException, ParseException
	{
		FileInputStream fi = new FileInputStream(new File(dl_program));

		Parser parser = new Parser(fi);
		Program program = parser.program();

//		for (Rule rule : program.getRules())
//		{
//			System.out.println(rule + " rel:" + rule.getRelationalType() + " agg:" + rule.isAggregate());
//		}
		rewrittenProgram = program.rewrite(true, true);
//		System.out.println(" REWRITTEN PROGRAM:");
//		for (Rule rule : rewrittenProgram.getRules())
//		{
//			System.out.println(rule + " rel:" + rule.getRelationalType() + " agg:" + rule.isAggregate());
//		}
		
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
		Pattern id = Pattern.compile("\\[\\[(\\d+?,\\d+?)\\]");			
		
		while((line = bfi.readLine()) != null)
		{
			Matcher m = id.matcher(line);	
			if(!m.find())	
			{
				System.out.println("---> No match found for " + input);
				throw new IOException("The input string did not match the regex pattern for super-vertex id."
						+ "Input = " + input);
			}	
			String[] sv_id = m.group(1).split(",");
//			System.out.println(Integer.parseInt(sv_id[0])+","+ Integer.parseInt(sv_id[1]));
			
			SuperVertexId s_id =  new SuperVertexId((short)Integer.parseInt(sv_id[0]), Integer.parseInt(sv_id[1]));
			Database data = readSuperVertexData(line);
			input_graph.put(s_id, data);
		}
		bfi.close();
	}
	
	private static Database readSuperVertexData(String input) throws JSONException, IOException
	{
		Metadata metadata = new Metadata();
		
		int[] vertexKeyFields = new int[]{0};
		Class[] vertexFieldTypes = new Class[]{Integer.class, Integer.class};
		Table vertexTable = new Table(vertexFieldTypes, vertexKeyFields, 10000); // <<- FIXME Initial size

		int[] outgoingNeighborsKeyFields = new int[]{0};
		Class[] outgoingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
		Table outgoingNeighborsTable = new Table(outgoingNeighborsFieldTypes, outgoingNeighborsKeyFields, 10000); // <<- FIXME Initial size

		int[] neighborSuperVerticesKeyFields = new int[]{0};
		Class[] neighborSuperVerticesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
		Table neighborSuperVerticesTable = new Table(neighborSuperVerticesFieldTypes, neighborSuperVerticesKeyFields, 100);
		
		Pattern sv_data = Pattern.compile("(\\[.*?\\])(\\[.*?\\])\\]");
		Pattern vdata = Pattern.compile("\\((\\d+,\\d+)\\)(\\[.*?\\])");
		Pattern edges = Pattern.compile("\\((\\d+,\\d+)\\)");
		Pattern sv_edges = Pattern.compile("\\((\\d+,\\d+,\\d+)\\)");
		Matcher sv_matcher = sv_data.matcher(input.substring(6));
		Matcher v_matcher = vdata.matcher("");
		Matcher e_matcher = edges.matcher("");
		Matcher sve_matcher = sv_edges.matcher("");
		
		if(sv_matcher.find())
		{
			v_matcher.reset(sv_matcher.group(1));
	
			//Read vertex data
			while(v_matcher.find())
				{
				//Read vertex id
				int[] vertexTuple = new int[2];
				String[] v_id = v_matcher.group(1).split(",");
				vertexTuple[0] = Integer.parseInt(v_id[0]);
				vertexTuple[1] = Integer.parseInt(v_id[1]);
				vertexTable.putTuple(new Tuple(vertexTuple));
				
				//Read edges of current vertex
				e_matcher.reset(v_matcher.group(2));
				while(e_matcher.find())
				{
					int[] edgeTuple = new int[3];
					edgeTuple[0] = vertexTuple[0];
					String[] e_id = e_matcher.group(1).split(",");
					edgeTuple[1] = Integer.parseInt(e_id[0]);
					edgeTuple[2] = Integer.parseInt(e_id[1]);	
					outgoingNeighborsTable.putTuple(new Tuple(edgeTuple));
				}
			}
			
			//Read neighbors of current super-vertex
			sve_matcher.reset(sv_matcher.group(2));
			while(sve_matcher.find())
			{
				String[] e_id = sve_matcher.group(1).split(",");
				int[] neighborSuperVertexTuple = new int[3];
				neighborSuperVertexTuple[0] = Integer.parseInt(e_id[0]);
				neighborSuperVertexTuple[1] = Integer.parseInt(e_id[1]);
				neighborSuperVertexTuple[2] = Integer.parseInt(e_id[2]);
				neighborSuperVerticesTable.putTuple(new Tuple(neighborSuperVertexTuple));
			}
		}
		else
		{
			throw new IOException("The input string did not match the regex pattern for vertex data."
					+ "Input = " + input);
		}
	
		metadata.setMetadata("vertices", vertexKeyFields, vertexFieldTypes);
		metadata.setMetadata("outgoingNeighbors", outgoingNeighborsKeyFields, outgoingNeighborsFieldTypes);
//		if(input.charAt(2) == '0' && input.charAt(4) == '0')
//		{
//			System.out.println((input.charAt(2) - '0')+","+ (input.charAt(4) - '0'));
			System.out.println("vertices = " + vertexTable);
			System.out.println("edges" + outgoingNeighborsTable);
//		}
		System.out.println("sv neighbors" + neighborSuperVerticesTable);
		Database database = new Database(metadata,-1);
		database.addDataTable("vertices", vertexTable);
		database.addDataTable("outgoingNeighbors", outgoingNeighborsTable);
		database.addDataTable("neighborSuperVertices", neighborSuperVerticesTable);
		
//		Date date = new Date();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//		String formattedDate = sdf.format(date);
//		System.out.println(formattedDate + "  Finished loading graph \n.");
		
		return database;
	}
}
