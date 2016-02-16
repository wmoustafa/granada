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
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import algebra.RelationalType;
import algebra.Rule;
import schema.Database;
import schema.Metadata;

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
//		try
//		{
//			System.out.println("*****************************************");
//			System.out.println("NOW AT VERTEX " + vertex.getId() + " AT SUPERSTEP " + getSuperstep());

			DatalogWorkerContext wc = getWorkerContext();
			boolean useSemiAsync = wc.useSemiAsync();
			boolean useSemiJoin = wc.useSemiJoin();
			boolean isPagerank = wc.getProgramName().equals("pagerank");
			Metadata metadata = wc.metadata;
			HashMap<Integer, SuperVertexId> neighbors = new HashMap<>();
//			aggregate("COMPUTE_INVOCATIONS", new LongWritable(1));			
			
			Database inputDatabase = vertex.getValue();
//			System.out.println("Vertex value = " + vertex.getValue());
			Database relationalDatabase = new Database();

			//Vicky: Combine messages from all neighbors into one message. Combine databases in per-table basis
			Database messagesDb = new Database();
			
			for (Database message : messages){
				messagesDb.combine2(message);				
			}
//			System.out.println("Message database after combining=" + messagesDb);
					
			//the following line is important. in case there were no messages, this has to be cleaned manually
			//, or other wise it will end up with messages from the past.
			//in case of sum aggregate, those messages will keep increasing the value of the things they join with
			inputDatabase.removeRelationalDeltaTables(); 
			
			Set<String> changedTables = new HashSet<>();

			Set<String> changed = inputDatabase.refresh(messagesDb);
			
			List<Rule> rulesToProcess = wc.getRulesToProcess();
			for (Rule rule : rulesToProcess)
			{
//				System.out.println("Evaluating " + rule +" with INPUT DATABASE: " + inputDatabase);
				Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase);
//				System.out.println("Output:" + outputDatabase);
				
				if (rule.getRelationalType() == RelationalType.NOT_RELATIONAL)
				{
					changed = inputDatabase.refresh(outputDatabase);
//					System.out.println("Refresh input with output: " + inputDatabase);
				}
				else
				{
//					System.out.println("Combine relationalDatase with output " );
//					System.out.println("Before combine: relational database: " + relationalDatabase);
					changed = relationalDatabase.combine2(outputDatabase);
					changedTables.addAll(changed);
//					System.out.println("After combine: relational database: " + relationalDatabase);
				}
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
					getDatabasesForEverySuperVertexEdgeBased(inputDatabase, neighbors, metadata);
				else if (useSemiAsync && !useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexWithMessages(inputDatabase, isPagerank);
				else if (useSemiAsync && useSemiJoin) 
					superVertexIdToDatabase = relationalDatabase.
					getDatabasesForEverySuperVertexWithMessagesEdgeBased(inputDatabase, isPagerank);
				for (Entry<SuperVertexId, Database> entry : superVertexIdToDatabase.entrySet())
				{
					SuperVertexId neighborId = entry.getKey();
					Database neighborDb = entry.getValue();
					sendMessage(neighborId, neighborDb);
//					System.out.println("SENT " + neighborDb + "TO NEIGHBOR: " + neighborId);
//					aggregate("SEND_RECORDS", new LongWritable(neighborDb.getDataTableByName("path_Y1727886952_OUTGOING").size()));
//					aggregate("SEND_MSG", new LongWritable(1));
				}
			}
			

			vertex.setValue(inputDatabase);
			vertex.voteToHalt();
//		}
//		catch (Exception e) 
//		{			
//			e.printStackTrace();
//		}
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
