package query.filter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import algebra.RelationalType;
import parser.Expression;
import schema.Database;
import schema.Metadata;
import schema.Table;
import schema.Tuple;
import utils.AggregationFunctionType;


public class GroupByFilter extends Filter {
	
	//Multimap<Tuple,String> derivation = HashMultimap.create();
	Map<Tuple, Integer> aggregateValues;
	Expression[] groupByFields;
	Class[] types;
	Expression aggregateField;
	Table outputTable;
	String outputTableName;
	int[] keyFields;
	RelationalType relationalType;
	boolean isRecursive;
	boolean isSourceNodeVariableUnncessary;
	AggregationFunctionType aggregationFunctionType;
	
	public GroupByFilter(String outputTableName, int[] keyFields, Expression[] groupByFields, AggregationFunctionType aggregationFunctionType, Expression aggregateField, boolean isRecursive, boolean isSourceNodeVariableUnncessary, RelationalType relationalType, Metadata metatadata)
	{
		
		this(outputTableName, keyFields, groupByFields, aggregationFunctionType, aggregateField, isRecursive, isSourceNodeVariableUnncessary, relationalType);
		types = new Class[groupByFields.length + 1];
		int j=0; for (Expression outputField :groupByFields) types[j++]=outputField.getType(metatadata);
		types[j] = Integer.class;
		metatadata.setMetadata(outputTableName, keyFields, types);
	}

	public GroupByFilter(String outputTableName, int[] keyFields, Expression[] groupByFields, AggregationFunctionType aggregationFunctionType, Expression aggregateField, boolean isRecursive, boolean isSourceNodeVariableUnncessary, RelationalType relationalType) {
		this.outputTableName = outputTableName;
		this.groupByFields = groupByFields;
		this.aggregateField = aggregateField;
		this.keyFields = keyFields;
		this.relationalType = relationalType;
		this.isRecursive = isRecursive;
		this.isSourceNodeVariableUnncessary = isSourceNodeVariableUnncessary;
		this.aggregationFunctionType = aggregationFunctionType;
//		System.out.println("GroupBy constructor. OutputTable = " + outputTableName + ", keyFields=" + Arrays.toString(keyFields));
	}
	
	public Filter duplicate() {
		Filter f = new GroupByFilter(outputTableName, keyFields, groupByFields, aggregationFunctionType, aggregateField, isRecursive, isSourceNodeVariableUnncessary, relationalType);
		((GroupByFilter)f).types = types; 
		if (nextFilter != null) f.nextFilter = nextFilter.duplicate();
		return f;
	}

	@Override
	public void open(Database inputDatabase, Database outputDatabase, Metadata metadata) {
		aggregateValues = new HashMap<Tuple, Integer>();

		if (!outputDatabase.exists(outputTableName))
		{
			outputTable = new Table(types, keyFields);
			Metadata.table_counter++;
			outputDatabase.addDataTable(outputTableName, outputTable);
		}
		else
			outputTable = outputDatabase.getDataTableByName(outputTableName);
		outputTable.setAggregationFunctionType(aggregationFunctionType);
		if (isRecursive) outputTable.setRecursive();
		if (isSourceNodeVariableUnncessary) outputTable.setSourceNodeVariableUnncessary();
		outputTable.setRelationalType(relationalType);
		Metadata.tuple_counter = 0;
	}

	@Override
	public void next() {
		int currentValue = (Integer)aggregateField.evaluate(cursor);
		int[] outputTuple = new int[groupByFields.length+1];
		for (int i = 0; i < groupByFields.length; i++) {
			outputTuple[i]=(Integer)groupByFields[i].evaluate(cursor);
		}
		outputTuple[groupByFields.length]=currentValue;		
		outputTable.addTuple(new Tuple(outputTuple));		
		Metadata.tuple_counter++;
	}
	
	public void close()
	{
//		System.out.println("[Size of GroupBy before close = "
//				+ MemoryMeasurer.measureBytes(this) + "].");
//		Footprint footprint = ObjectGraphMeasurer.measure(this);
//		System.out.println("GroupBy = " +footprint);
//		System.out.println("GroupBy tuples in output table = " + outputTable.getData().getSizeRecursively());
		if (nextFilter!=null) nextFilter.close();
	}
	
	
	public String toString()
	{
		StringBuffer s = new StringBuffer("GROUP BY FILTER\n");
		s.append("  " + outputTableName + "\n");
		s.append("  GroupByFields" + Arrays.toString(groupByFields) + "\n");
		s.append("  Execution time: ");
		if (nextFilter!=null)
			s.append(executionTime-nextFilter.executionTime);
		else
			s.append(executionTime);
		s.append("\n");
		return s.toString();
	}
}
