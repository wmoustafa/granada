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

import parser.Parser;
import algebra.Program;
import algebra.Rule;
public class DatalogMasterCompute extends DefaultMasterCompute {
	
	Set<String> aggregators = new HashSet<String>();
	Configuration conf;
	
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
		if (getSuperstep() > 0) if (this.<BooleanWritable>getAggregatedValue("HALT_COMPUTATION").get())
			haltComputation();
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		try 
		{
			conf = getConf();

			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("hdfs://HOSTNAME:8020/user/hadoop/input/" + getProgramName() + ".txt"));

			Parser parser = new Parser(in);
			Program program = parser.program();

			Program rewrittenProgram = program.rewrite(useSemiJoin(), useEagerAggregation());

			for (Rule rule : rewrittenProgram.getRules())
			{
				String aggregator = rule.getHead().getName();
				registerAggregator(aggregator, BooleanOrAggregator.class);
				aggregators.add(aggregator);
			}
			registerAggregator("HALT_COMPUTATION", BooleanAndAggregator.class);

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
