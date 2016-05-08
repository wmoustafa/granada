package query.filter;

import java.util.Arrays;

import algebra.RelationalType;
import parser.Expression;
import schema.Database;
import schema.Metadata;
import schema.Table;
import schema.Tuple;
import utils.AggregationFunctionType;

public class ProjectFilter extends Filter {
	
	String outputTableName;
	Expression[] outputFields;
	Table outputTable;
	int[] keyFields;
	Class[] types;
	AggregationFunctionType aggregationFunctionType;
	boolean isRecursive;
	boolean isSourceNodeVariableUnncessary;
	RelationalType relationalType;
	
	public ProjectFilter(String outputTableName, int[] keyFields, Expression[] outputFields, AggregationFunctionType aggregationFunctionType, boolean isRecursive, boolean isSourceNodeVariableUnncessary, RelationalType relationalType, Metadata metadata)
	{
		this(outputTableName, keyFields, outputFields, aggregationFunctionType, isRecursive, isSourceNodeVariableUnncessary, relationalType);
		types = metadata.getTypes(outputFields);
		metadata.setMetadata(outputTableName, keyFields, types);
	}

	public ProjectFilter(String outputTableName, int[] keyFields, Expression[] outputFields, AggregationFunctionType aggregationFunctionType, boolean isRecursive, boolean isSourceNodeVariableUnncessary, RelationalType relationalType)
	{
		this.outputTableName = outputTableName;
		this.outputFields = outputFields;
		this.keyFields = keyFields;
		this.aggregationFunctionType = aggregationFunctionType;
		this.isRecursive = isRecursive;
		this.isSourceNodeVariableUnncessary = isSourceNodeVariableUnncessary;
		this.relationalType = relationalType;		
	}
	
	public Filter duplicate()
	{
		Filter f = new ProjectFilter(outputTableName, keyFields, outputFields, aggregationFunctionType, isRecursive, isSourceNodeVariableUnncessary, relationalType);
		((ProjectFilter)f).types = types; 
		if (nextFilter != null) f.nextFilter = nextFilter.duplicate();
		return f;
	}
	
	@Override
	public void open(Database inputDatabase, Database outputDatabase, Metadata metadata) {

		if (!outputDatabase.exists(outputTableName))
		{
			outputTable = new Table(types, keyFields);
			outputDatabase.addDataTable(outputTableName, outputTable);
		}
		else outputTable = outputDatabase.getDataTableByName(outputTableName);
		outputTable.setRelationalType(relationalType);
		outputTable.setAggregationFunctionType(aggregationFunctionType);
		if (isRecursive) outputTable.setRecursive();
		if (isSourceNodeVariableUnncessary) outputTable.setSourceNodeVariableUnncessary();
	}

	@Override
	public void next() {
		Tuple projection = cursor.evaluate(outputFields);
		outputTable.addTuple(projection);
	}

	public void close()
	{
	}

	public String toString()
	{
		StringBuffer s;
		s = new StringBuffer("PROJECT FILTER\n");
		s.append("  " + outputTableName + "\n");
		s.append("  Execution time: ");
		if (nextFilter!=null)
			s.append(executionTime-nextFilter.executionTime);
		else
			s.append(executionTime);
		s.append("\n");
		s.append("  Output Expressions:"+Arrays.toString(outputFields)+"\n");
		return s.toString();
	}
	
}
