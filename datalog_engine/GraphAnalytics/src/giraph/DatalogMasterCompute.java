package giraph;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import parser.Parser;
import algebra.Program;
import algebra.Rule;
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
//			long ONE_MILLION = 1000000;
//			long COMBINE_MSG = this.<LongWritable>getAggregatedValue("COMBINE_MSG").get();
//			long REFRESH_DB = this.<LongWritable>getAggregatedValue("REFRESH_DB").get();
//			long EVALUATE_RULE = this.<LongWritable>getAggregatedValue("EVALUATE_RULE").get();
			long SEND_MSG = this.<LongWritable>getAggregatedValue("SEND_MSG").get();
			long SEND_RECORDS = this.<LongWritable>getAggregatedValue("SEND_RECORDS").get();
			long COMPUTE_INVOCATIONS = this.<LongWritable>getAggregatedValue("COMPUTE_INVOCATIONS").get();
//			long COMPUTE_TIME = this.<LongWritable>getAggregatedValue("COMPUTE_TIME").get();
//			long PARTITION_MSG = this.<LongWritable>getAggregatedValue("PARTITION_MSG").get();
//			long COMBINE_OUTPUT = this.<LongWritable>getAggregatedValue("COMBINE_OUTPUT").get();
//			long REFRESH_OUTPUT = this.<LongWritable>getAggregatedValue("REFRESH_OUTPUT").get();
//			long REMOVE_TABLES = this.<LongWritable>getAggregatedValue("REMOVE_TABLES").get();
//			System.out.println("TOTAL Combine messages=" + (COMBINE_MSG/ONE_MILLION) 
//					+ ", TOTAL remove tables = " + (REMOVE_TABLES/ONE_MILLION)
//					+ ", TOTAL Refresh db =" + (REFRESH_DB/ONE_MILLION)
//					+ ", TOTAL Evaluate rule = " + (EVALUATE_RULE/ONE_MILLION)
//					+ ", TOTAL Refresh output = " + (REFRESH_OUTPUT/ONE_MILLION)
//					+ ", TOTAL COMBINE output = " + (COMBINE_OUTPUT/ONE_MILLION)
//					+ ", TOTAL PARTITION message = " + (PARTITION_MSG/ONE_MILLION)
//					+ ", TOTAL Send message = " + (SEND_MSG/ONE_MILLION)
//					+ ", TOTAL Compute Time =" + (COMPUTE_TIME/ONE_MILLION));

			sum_msg+=SEND_MSG;
			sum_records+=SEND_RECORDS;
			sum_invocations+=COMPUTE_INVOCATIONS;
			
			
			System.out.println("2TOTAL message = " + sum_msg);
			System.out.println("2TOTAL number of records = " + sum_records);
			System.out.println("2TOTAL compute invocations = " + sum_invocations);
			
			
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
			FSDataInputStream in = fs.open(new Path("hdfs://b09-11.sysnet.ucsd.edu:8020/user/hadoop/input/" + getProgramName() + ".txt"));

			Parser parser = new Parser(in);
			Program program = parser.program();

			Program rewrittenProgram = program.rewrite(useSemiJoin(), useEagerAggregation());

//			//System.out.println(" REWRITTEN PROGRAM:");
			for (Rule rule : rewrittenProgram.getRules())
			{
				String aggregator = rule.getHead().getName();
//				//System.out.println(rule + " rel:" + rule.getRelationalType() + " agg:" + rule.isAggregate());
				registerAggregator(aggregator, BooleanOrAggregator.class);
				aggregators.add(aggregator);
			}
			registerAggregator("HALT_COMPUTATION", BooleanAndAggregator.class);
//			registerAggregator("COMBINE_MSG", LongSumAggregator.class);
//			registerAggregator("EVALUATE_RULE", LongSumAggregator.class);
//			registerAggregator("REFRESH_DB", LongSumAggregator.class);
			registerAggregator("SEND_MSG", LongSumAggregator.class);
			registerAggregator("SEND_RECORDS", LongSumAggregator.class);
			registerAggregator("COMPUTE_INVOCATIONS", LongSumAggregator.class);
//			registerAggregator("COMPUTE_TIME", LongSumAggregator.class);
//			registerAggregator("PARTITION_MSG", LongSumAggregator.class);
//			registerAggregator("COMBINE_OUTPUT", LongSumAggregator.class);
//			registerAggregator("REFRESH_OUTPUT", LongSumAggregator.class);
//			registerAggregator("REMOVE_TABLES", LongSumAggregator.class);
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
