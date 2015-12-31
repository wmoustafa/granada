package giraph;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.xml.crypto.Data;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import parser.DatalogVariable;
import parser.Expression;
import parser.IntegerConst;
import parser.Operation;
import parser.Parser;
import query.filter.Filter;
import query.filter.ScanFilter;
import algebra.Predicate;
import algebra.Program;
import algebra.RelationalType;
import algebra.Rule;
import schema.Database;
import schema.Table;

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
		try
		{
			DatalogWorkerContext wc = getWorkerContext();
			boolean useSemiAsync = wc.useSemiAsync();
			boolean useSemiJoin = wc.useSemiJoin();
			boolean isPagerank = wc.getProgramName().equals("pagerank");
			
			Database inputDatabase = vertex.getValue();
			Database relationalDatabase = new Database();

			Database messagesDb = new Database();
			for (Database message : messages)
				messagesDb.combine2(message);
			//the following line is important. in case there were no messages, this has to be cleaned manually
			//, or other wise it will end up with messages from the past.
			//in case of sum aggregate, those messages will keep increasing the value of the things they join with
			inputDatabase.removeRelationalDeltaTables(); 
			Set<String> changedTables = new HashSet<>();

			Set<String> changed = inputDatabase.refresh(messagesDb);
						
			List<Rule> rulesToProcess = wc.getRulesToProcess();
			for (Rule rule : rulesToProcess)
			{
				Database outputDatabase = rule.getEvaluationPlan().duplicate().evaluate(inputDatabase);
				if (rule.getRelationalType() == RelationalType.NOT_RELATIONAL)
				{
					changed = inputDatabase.refresh(outputDatabase);
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
				Map<SuperVertexId, Database> superVertexIdToDatabase = null;
				if (!useSemiAsync && !useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertex(inputDatabase);
				else if (!useSemiAsync && useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertexUseSemiJoin(inputDatabase);
				else if (useSemiAsync && !useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertexUseSemiAsync(inputDatabase, isPagerank);
				else if (useSemiAsync && useSemiJoin) superVertexIdToDatabase = relationalDatabase.getDatabasesForEverySuperVertexUseSemiJoinSemiAsync(inputDatabase, isPagerank);
				for (Entry<SuperVertexId, Database> entry : superVertexIdToDatabase.entrySet())
				{
					SuperVertexId neighborId = entry.getKey();
					Database neighborDb = entry.getValue();
					sendMessage(neighborId, neighborDb);
				}
			}


			vertex.setValue(inputDatabase);
			//vertex.voteToHalt();
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

}
