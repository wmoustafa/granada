package query.filter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
			outputTable = new Table(outputTableName, types, keyFields, metadata);
			outputDatabase.addDataTable(outputTableName, outputTable);
		}
		else
			outputTable = outputDatabase.getDataTableByName(outputTableName);
		outputTable.setAggregationFunctionType(aggregationFunctionType);
		if (isRecursive) outputTable.setRecursive();
		if (isSourceNodeVariableUnncessary) outputTable.setSourceNodeVariableUnncessary();
		outputTable.setRelationalType(relationalType);
		metadata.setMetadata(outputTableName, keyFields, types, relationalType, isRecursive, isSourceNodeVariableUnncessary, aggregationFunctionType);
	}

	@Override
	public void next() {
		Tuple group = cursor.evaluate(groupByFields);
		Integer agg = aggregateValues.get(group);
		if (agg==null) 
		{
			if (aggregationFunctionType == AggregationFunctionType.SUM || aggregationFunctionType == AggregationFunctionType.COUNT) agg=0;
			if (aggregationFunctionType == AggregationFunctionType.MIN) agg=Integer.MAX_VALUE;
			if (aggregationFunctionType == AggregationFunctionType.MAX) agg=Integer.MIN_VALUE;
		}
		
		int currentValue = (Integer)aggregateField.evaluate(cursor);
//		System.out.println("GroupBy currentValue = " + currentValue);
		if (aggregationFunctionType == AggregationFunctionType.SUM)
			agg += currentValue;
		else if (aggregationFunctionType == AggregationFunctionType.COUNT)
			agg ++;
		else if (aggregationFunctionType == AggregationFunctionType.MIN)
			agg = currentValue < agg ? currentValue : agg;
		else if (aggregationFunctionType == AggregationFunctionType.MAX)
		{
			agg = currentValue > agg ? currentValue : agg;
		}
		aggregateValues.put(group, agg);
//		System.out.println("GroupBy aggreagateValues = " + aggregateValues);
		//derivation.put(group, cursor.toString());
	}
	
	public void close()
	{
		for (Tuple values : aggregateValues.keySet())
		{
			int[] outputTuple = new int[values.toArray().length+1];
			int i=0; 
			for (int o : values.toArray()) outputTuple[i++]=o;
			outputTuple[i]=aggregateValues.get(values);			
			outputTable.addTuple(new Tuple(outputTuple));
			////System.out.println(Arrays.toString(outputTuple) + " is derived from: " + derivation.get(values));
		}
		//Log.DEBUG("***************************************************");
			//Log.DEBUG(this);
		if (nextFilter!=null) nextFilter.close();
	}
	public String toString()
	{
		StringBuffer s = new StringBuffer("GROUP BY FILTER\n");
		s.append("  " + outputTableName + "\n");
		s.append("  GroupByFields" + groupByFields + "\n");
		s.append("  Execution time: ");
		if (nextFilter!=null)
			s.append(executionTime-nextFilter.executionTime);
		else
			s.append(executionTime);
		s.append("\n");
		return s.toString();
	}
}
