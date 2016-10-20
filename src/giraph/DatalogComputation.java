package giraph;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import algebra.RelationalType;
import algebra.Rule;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import schema.Database;
import schema.Metadata;

import objectexplorer.MemoryMeasurer;
import objectexplorer.ObjectGraphMeasurer;
import objectexplorer.ObjectGraphMeasurer.Footprint;

public class DatalogComputation extends BasicComputation<SuperVertexId, Database, NullWritable, Database> {
	private static final Logger LOG =
			Logger.getLogger(DatalogComputation.class);
	

	public void preSuperstep()
	{
		
	}
	@Override
	public void compute(
			Vertex<SuperVertexId, Database, NullWritable> vertex,
			Iterable<Database> messages) throws IOException {
			StringBuffer sb = null;
			DatalogWorkerContext wc = getWorkerContext();
			boolean useSemiAsync = wc.useSemiAsync();
			boolean useSemiJoin = wc.useSemiJoin();
			boolean isPagerank = wc.getProgramName().equals("pagerank");
			Int2ObjectOpenHashMap<SuperVertexId> neighbors = new Int2ObjectOpenHashMap<SuperVertexId>();
			Metadata metadata = wc.metadata;
			
			
			Database inputDatabase = vertex.getValue();
			if(vertex.getId().getVertexId() == 0  ){
				sb = new StringBuffer();
				sb.append("***************************************** \n");
				sb.append("NOW AT VERTEX " + vertex.getId() + " AT SUPERSTEP " + getSuperstep() + "\n\n");				
				sb.append("[Size of input database = " + MemoryMeasurer.measureBytes(inputDatabase) + "].\n");
				Footprint footprint = ObjectGraphMeasurer.measure(inputDatabase);
				sb.append("InputDatabase = " +footprint + "\n");
				//FIXME seems to be wrong
				//sb.append("[Size of incoming messages  = " + MemoryMeasurer.measureBytes(messages) + "].\n");
			}
			Database relationalDatabase = new Database();

			//Vicky: Combine messages from all neighbors into one message. Combine databases in per-table basis, apply aggregation on tuples with the same key
			Database messagesDb = new Database();
			
			for (Database message : messages){
				messagesDb.combine2(message);				
			}
			if(vertex.getId().getVertexId() == 0){
				sb.append("[Size of messagesDB after combine = " + MemoryMeasurer.measureBytes(messagesDb) + "]. \n");
			}
			assert(!messagesDb.isEmpty());
					
			Set<String> changedTables = new HashSet<>();
			Set<String> changed = inputDatabase.refresh(messagesDb); //<-------------------- refresh
			List<Rule> rulesToProcess = wc.getRulesToProcess();
			
			for (Rule rule : rulesToProcess)
			{
				Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase, metadata);
				if(vertex.getId().getVertexId() == 0){
					sb.append("[Size of outputDatabase = " + MemoryMeasurer.measureBytes(outputDatabase) +" ] \n");
				}
				
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
			if(vertex.getId().getVertexId() == 0){
				sb.append("[Size of inputDatabase after plan evaluation = " + MemoryMeasurer.measureBytes(inputDatabase) + "].\n");
				Footprint footprint = ObjectGraphMeasurer.measure(inputDatabase);
				sb.append("InputDatabase = " +footprint + "\n");
				sb.append("[Size of relationalDatabase = " +  MemoryMeasurer.measureBytes(relationalDatabase) + "].\n");
			}
			
			for (String table : changedTables)
				aggregate(table, new BooleanWritable(true));

			if (!relationalDatabase.isEmpty())
			{
				Map<SuperVertexId, Database> superVertexIdToDatabase = null;
				if (!useSemiAsync && !useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertex(inputDatabase);
				else if (!useSemiAsync && useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexEdgeBased(inputDatabase, neighbors);
				else if (useSemiAsync && !useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexWithMessages(inputDatabase, isPagerank);
				else if (useSemiAsync && useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexWithMessagesEdgeBased(inputDatabase, isPagerank);
				if(vertex.getId().getVertexId() == 0){
					sb.append("Map supervertex to message = " + MemoryMeasurer.measureBytes(superVertexIdToDatabase) + "] \n");
				}
				
				for (Entry<SuperVertexId, Database> entry : superVertexIdToDatabase.entrySet())
				{
					SuperVertexId neighborId = entry.getKey();
					Database neighborDb = entry.getValue();
//					sb.append("[Message send size= " + MemoryMeasurer.measureBytes(neighborDb) + "]");
					sendMessage(neighborId, neighborDb);
				}
			}
			if(vertex.getId().getVertexId() == 0){
				sb.append("[AFTER Total size of supervertex = " + MemoryMeasurer.measureBytes(vertex.getValue()) + "]. \n");
				System.out.println(sb.toString());
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
