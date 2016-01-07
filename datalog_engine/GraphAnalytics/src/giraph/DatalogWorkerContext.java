package giraph;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import parser.Parser;
import schema.Metadata;
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
	
	private static long COMBINE_MSG;
	private static long EVALUATE_RULE;
	
	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		try {
			////System.out.println("Available processors (cores): " + 
			//		Runtime.getRuntime().availableProcessors());
			conf = getConf();

			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("hdfs://jedi2.nec-labs.com:8020/user/hadoop/input/" + getProgramName() + ".txt"));

			Parser parser = new Parser(in);
			Program program = parser.program();

			System.out.println("Program = " + getProgramName());
			rewrittenProgram = program.rewrite(useSemiJoin(), useEagerAggregation());
			
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
			rulesToProcess = g.getFirstToProcess();
		}
		else
		{
			////System.out.println("predicatesToProcess " + rulesToProcess);
			changed = new HashMap<String, Boolean>();
			for (Rule rule : rulesToProcess)
				changed.put(rule.getHead().getName(), this.<BooleanWritable>getAggregatedValue(rule.getHead().getName()).get());
			////System.out.println("changed" + changed);
			rulesToProcess = g.getNextToProcess(changed);
		}
		////System.out.println(this.getSuperstep());
//		System.out.println("-----> Now going to process: " +rulesToProcess);
		////System.out.println("Free memory: " + Runtime.getRuntime().freeMemory()/1024/1024);
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
		long ONE_MILLION = 1000000;
	      
//	      COMBINE_MSG = this.<LongWritable>getAggregatedValue("COMBINE_MSG").get();
//	      EVALUATE_RULE = this.<LongWritable>getAggregatedValue("EVALUATE_RULE").get();
//	      
//	      System.out.println("TOTAL Combine messages=" + (COMBINE_MSG/ONE_MILLION));
//	      System.out.println("TOTAL Evaluate rule = " + (EVALUATE_RULE/ONE_MILLION));
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
