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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import schema.Database;
import schema.Metadata;
import algebra.RelationalType;
import algebra.Rule;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class DatalogComputation extends BasicComputation<SuperVertexId, Database, NullWritable, Database> {
	private static final Logger LOG =
			Logger.getLogger(DatalogComputation.class);
	
	long compute_start, compute_end;

	public void preSuperstep()
	{
		
	}
	@Override
	public void compute(
			Vertex<SuperVertexId, Database, NullWritable> vertex,
			Iterable<Database> messages) throws IOException {
		try
		{
//			System.out.println("*****************************************");
//			System.out.println("NOW AT VERTEX " + vertex.getId() + " AT SUPERSTEP " + getSuperstep());

			compute_start = System.nanoTime();
			DatalogWorkerContext wc = getWorkerContext();
			boolean useSemiAsync = wc.useSemiAsync();
			boolean useSemiJoin = wc.useSemiJoin();
			boolean isPagerank = wc.getProgramName().equals("pagerank");
			Metadata metadata = wc.metadata;
			HashMap<Integer, SuperVertexId> neighbors = new HashMap<>();
			
			long start, end;
			
			
			Database inputDatabase = vertex.getValue();
//			System.out.println("Vertex value = " + vertex.getValue());
			Database relationalDatabase = new Database();

			//Vicky: Combine messages from all neighbors into one message. Combine databases in per-table basis
			Database messagesDb = new Database();
//			start = System.nanoTime();
			for (Database message : messages)
				messagesDb.combine2(message);
//			end = System.nanoTime();
//			aggregate("COMBINE_MSG", new LongWritable(end-start));		
					
			//the following line is important. in case there were no messages, this has to be cleaned manually
			//, or other wise it will end up with messages from the past.
			//in case of sum aggregate, those messages will keep increasing the value of the things they join with
//			start = System.nanoTime();
			inputDatabase.removeRelationalDeltaTables(); 
//			end = System.nanoTime();
//			aggregate("REMOVE_TABLES", new LongWritable(end-start));
			
			Set<String> changedTables = new HashSet<>();

//			System.out.println("Got messages: " + messagesDb);
//			start = System.nanoTime();
			Set<String> changed = inputDatabase.refresh(messagesDb);
//			end = System.nanoTime();
//			aggregate("REFRESH_DB", new LongWritable(end-start));			
			
			List<Rule> rulesToProcess = wc.getRulesToProcess();
			for (Rule rule : rulesToProcess)
			{
//				System.out.println("Evaluating " + rule +" with INPUT DATABASE: " + inputDatabase);
//				start = System.nanoTime();
				Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase, metadata);
//				end = System.nanoTime();
//				aggregate("EVALUATE_RULE", new LongWritable(end-start));		
				
//				System.out.println("Output:" + outputDatabase);
				if (rule.getRelationalType() == RelationalType.NOT_RELATIONAL)
				{
//					start = System.nanoTime();
//					System.out.println("Refresh input database with outpout" );
					changed = inputDatabase.refresh(outputDatabase);
//					end = System.nanoTime();
//					aggregate("REFRESH_OUTPUT", new LongWritable(end-start));
					////System.out.println("Subtracted input from output: " + outputDatabase);
					////System.out.println("After combining: " + inputDatabase);
					////System.out.println("CHANGED:" + changed);
				}
				else
				{
//					System.out.println("Combine relationalDatase with output " );
//					System.out.println("Before combine: relational database: " + relationalDatabase);
//					start = System.nanoTime();
					changed = relationalDatabase.combine2(outputDatabase);
					changedTables.addAll(changed);
//					end = System.nanoTime();
//					aggregate("COMBINE_OUTPUT", new LongWritable(end-start));
//					System.out.println("After combine: relational database: " + relationalDatabase);
				}
			}
			
			////System.out.println("ALLCHANGED:" + changedTables);
			for (String table : changedTables)
				aggregate(table, new BooleanWritable(true));

//			start = System.nanoTime();
			if (!relationalDatabase.isEmpty())
			{
				Map<SuperVertexId, Database> superVertexIdToDatabase = null;
				if (!useSemiAsync && !useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertex(inputDatabase);
				else if (!useSemiAsync && useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertexEdgeBased(inputDatabase, neighbors, metadata);
				else if (useSemiAsync && !useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertexWithMessages(inputDatabase, isPagerank);
				else if (useSemiAsync && useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertexWithMessagesEdgeBased(inputDatabase, isPagerank);
//				end = System.nanoTime();
//				aggregate("PARTITION_MSG", new LongWritable(end-start));
				start = System.nanoTime();
				for (Entry<SuperVertexId, Database> entry : superVertexIdToDatabase.entrySet())
				{
					SuperVertexId neighborId = entry.getKey();
					Database neighborDb = entry.getValue();
					sendMessage(neighborId, neighborDb);
//					System.out.println(neighborDb.getTableSizes());
//					System.out.println("SENT " + neighborDb + "TO NEIGHBOR: " + neighborId);
				}
//				end = System.nanoTime();
//				aggregate("SEND_MSG", new LongWritable(end-start));
			}
			

			vertex.setValue(inputDatabase);
			//vertex.voteToHalt();
//			compute_end = System.nanoTime();
//			aggregate("COMPUTE_TIME", new LongWritable(compute_end-compute_start));
		}
		catch (Exception e) 
		{			
			e.printStackTrace();
		}
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
