package giraph;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import algebra.RelationalType;
import algebra.Rule;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import schema.Database;
import schema.Metadata;


public class DatalogComputation extends BasicComputation<IntWritable, Database, NullWritable, Database> {
	private static final Logger LOG =
			Logger.getLogger(DatalogComputation.class);
	

	public void preSuperstep()
	{
		
	}
	@Override
	public void compute(
			Vertex<IntWritable, Database, NullWritable> vertex,
			Iterable<Database> messages) throws IOException {
//			StringBuffer sb = null;
//			if(vertex.getId().getVertexId() > 0 && vertex.getId().getVertexId() < 10  ){
//				sb = new StringBuffer();
//				sb.append("***************************************** \n");
//				sb.append("NOW AT VERTEX " + vertex.getId() + " AT SUPERSTEP " + getSuperstep() + "\n");	
//				sb.append("[BEFORE Total size of supervertex = " + MemoryMeasurer.measureBytes(vertex.getValue()) + "]. ");
//				sb.append("Detailed table sizes of Database: "+ vertex.getValue().printTableSizes());
//			}
		
//		System.out.println("NOW AT VERTEX " + vertex.getId() + " AT SUPERSTEP " + getSuperstep());
		

			DatalogWorkerContext wc = getWorkerContext();
			boolean useSemiAsync = wc.useSemiAsync();
			boolean useSemiJoin = wc.useSemiJoin();
			boolean isPagerank = wc.getProgramName().equals("pagerank");
			Metadata metadata = wc.metadata;
			
			
			Database inputDatabase = vertex.getValue();
			Database relationalDatabase = new Database();

			//Vicky: Combine messages from all neighbors into one message. Combine databases in per-table basis, apply aggregation on tuples with the same key
			Database messagesDb = new Database();
			
			for (Database message : messages){
				messagesDb.combine2(message);
				//inputDatabase.combine2(message); //Vicky FIXME efficiency optimization
			}

			//assert(!messagesDb.isEmpty());
					
			Set<String> changedTables = new HashSet<>();
			//Set<String> changed = new HashSet<>();
			Set<String> changed = inputDatabase.refresh(messagesDb); //Vicky FIXME efficiency optimization
			List<Rule> rulesToProcess = wc.getRulesToProcess();
			
			for (Rule rule : rulesToProcess)
			{
				Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase, metadata);
								
				//TODO for PageRank improvement to remove intermediate results
				if (wc.getProgramName().equals("page_rank")) {   
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
				aggregate(table, new BooleanWritable(true));

			if (!relationalDatabase.isEmpty())
			{
				HashMap<Integer,Database> superVertexIdToDatabase = null;
				if (!useSemiAsync && !useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertex(inputDatabase);
				else if (!useSemiAsync && useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexEdgeBased(inputDatabase);
				else if (useSemiAsync && !useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexWithMessages(inputDatabase, isPagerank);
				else if (useSemiAsync && useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexWithMessagesEdgeBased(inputDatabase, isPagerank);
				
				for (Entry<Integer, Database> entry : superVertexIdToDatabase.entrySet())
				{
					int neighborId = entry.getKey();
					Database neighborDb = entry.getValue();
//					sb.append("[Message send size= " + MemoryMeasurer.measureBytes(neighborDb) + "]");
					sendMessage(new IntWritable(neighborId), neighborDb);
				}
			}
			
			vertex.setValue(inputDatabase);
			vertex.voteToHalt();
			
	}

	Map<String,Boolean> stringToMap(String str)
	{
		Gson gson = new Gson();
		Type stringBooleanMap = new TypeToken<Map<String,Boolean>>(){}.getType();
		Map<String,Boolean> map = gson.fromJson(str.replace(";", ","), stringBooleanMap);
		return map;
	}

	String mapToString(Map<String,Boolean> map)
	{
		Gson gson = new Gson();
		String str = gson.toJson(map);
		return str.replace(",", ";");
	}

	void changeTableNames(Database database) {
		
	}
	
}
