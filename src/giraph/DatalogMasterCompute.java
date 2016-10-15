package giraph;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;

import algebra.Program;
import algebra.Rule;
import parser.Parser;
public class DatalogMasterCompute extends DefaultMasterCompute {
	
	Set<String> aggregators = new HashSet<String>();
	Configuration conf;
	long sum_msg = 0, sum_records = 0, sum_invocations = 0;
	
	public void compute() {
		/*if (getSuperstep() > 0)
		{
			boolean halt = true;
			for (String aggregator : aggregators)
				if (this.<BooleanWritable>getAggregatedValue(aggregator).get())
				{
					halt = false;
					break;
				}
			if (halt) haltComputation();
		}*/
		
		if (getSuperstep() > 0) {
			if (this.<BooleanWritable>getAggregatedValue("HALT_COMPUTATION").get()){
				haltComputation();
			}
		}
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		try 
		{
			conf = getConf();

			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("hdfs://b09-26.sysnet.ucsd.edu:9000/user/hadoop/input/" + getProgramName() + ".txt"));

			Parser parser = new Parser(in);
			Program program = parser.program();

			Program rewrittenProgram = program.rewrite(useSemiJoin(), useEagerAggregation());

			System.out.println(" REWRITTEN PROGRAM:");
			for (Rule rule : rewrittenProgram.getRules())
			{
				String aggregator = rule.getHead().getName();
				System.out.println(rule + " rel:" + rule.getRelationalType() + " agg:" + rule.isAggregate());
				registerAggregator(aggregator, BooleanOrAggregator.class);
				aggregators.add(aggregator);
			}
			registerAggregator("HALT_COMPUTATION", BooleanAndAggregator.class);
//			registerAggregator("SEND_MSG", LongSumAggregator.class);
//			registerAggregator("SEND_RECORDS", LongSumAggregator.class);
//			registerAggregator("COMPUTE_INVOCATIONS", LongSumAggregator.class);			
//			registerAggregator("COMBINE_MSG", LongSumAggregator.class);
//			registerAggregator("EVALUATE_RULE", LongSumAggregator.class);
//			registerAggregator("REFRESH_DB", LongSumAggregator.class);
//			registerAggregator("COMPUTE_TIME", LongSumAggregator.class);
//			registerAggregator("PARTITION_MSG", LongSumAggregator.class);
//			registerAggregator("COMBINE_OUTPUT", LongSumAggregator.class);
//			registerAggregator("REFRESH_OUTPUT", LongSumAggregator.class);
//			registerAggregator("REMOVE_TABLES", LongSumAggregator.class);
//			registerAggregator("SEND_MSG_TIME", LongSumAggregator.class);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	public boolean useSemiJoin()
	{
		return conf.getBoolean("datalog.useSemiJoin", false);
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
