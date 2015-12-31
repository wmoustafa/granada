package giraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.job.HaltApplicationUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;

import parser.Parser;
import query.filter.Filter;
import schema.Database;
import schema.Metadata;
import schema.Table;
import algebra.Program;
import algebra.Rule;

public class DatalogWorkerContext extends WorkerContext {

	DatalogDependencyGraph g;
	List<Rule> rulesToProcess;
	Program rewrittenProgram;
	Map<String,Boolean> changed;
	Metadata metadata;
	Configuration conf;

	boolean firstVertex = true;
	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		try {
			conf = getConf();

			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("hdfs://HOSTNAME:8020/user/hadoop/input/" + getProgramName() + ".txt"));

			Parser parser = new Parser(in);
			Program program = parser.program();

			rewrittenProgram = program.rewrite(useSemiJoin(), useEagerAggregation());
			
			g = new DatalogDependencyGraph(rewrittenProgram);
			g.setRecursivePredicatesForRules();
			metadata = new Metadata();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public boolean isFirstVertex()
	{
		boolean tmp = firstVertex;
		firstVertex = false;
		return tmp;
	}
	@Override
	public void preSuperstep() {
		if (getSuperstep() == 0)
		{
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

			rulesToProcess = g.getFirstToProcess();
		}
		else
		{
			changed = new HashMap<String, Boolean>();
			for (Rule rule : rulesToProcess)
				changed.put(rule.getHead().getName(), this.<BooleanWritable>getAggregatedValue(rule.getHead().getName()).get());
			rulesToProcess = g.getNextToProcess(changed);
		}
		for (Rule rule : rulesToProcess)
			rule.generateEvaluationPlan(null, metadata);
		aggregate("HALT_COMPUTATION", new BooleanWritable(rulesToProcess.isEmpty()));
		firstVertex = true;
		
	}	

	@Override
	public void postSuperstep() {
		
	}
	

	@Override
	public void postApplication() {
	}

	public Program getRewrittenProgram()
	{
		return rewrittenProgram;
	}

	public List<Rule> getRulesToProcess()
	{
		return rulesToProcess;
	}
	
	public boolean useSemiJoin()
	{
		return conf.getBoolean("datalog.useSemiJoin", false);
	}
		
	public boolean useSemiAsync()
	{
		return conf.getBoolean("datalog.useSemiAsync", false);
	}
	
	public boolean useEagerAggregation()
	{
		return conf.getBoolean("datalog.useEagerAggregation", true);
	}
	
	public String getProgramName()
	{
		return conf.get("datalog.programName");
	}


}
