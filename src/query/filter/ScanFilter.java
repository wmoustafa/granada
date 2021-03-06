
package query.filter;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;

import maputil.GoogleMultimap;
import maputil.Multimap;
import evaluation.TableAlias;
import parser.Expression;
import schema.Database;
import schema.Table;
import schema.Tuple;

public class ScanFilter extends Filter {
	
	TableAlias inputTableAlias;
	Table inputTable;

	public ScanFilter(TableAlias inputTableAlias, Expression[] filterConditions)
	{
		this.inputTableAlias = inputTableAlias;
		this.filterConditions = filterConditions;
	}
	
	public Filter duplicate()
	{
		Filter f = new ScanFilter(inputTableAlias, filterConditions);
		if (nextFilter != null) f.nextFilter = nextFilter.duplicate();
		return f;
	}
	
	public void open(Database inputDatabase, Database outputDatabase)
	{
		inputTable = inputDatabase.getDataTableByName(inputTableAlias.tableName);
		if (nextFilter!=null) 
		{
			nextFilter.setInputCursor(cursor);
			nextFilter.open(inputDatabase, outputDatabase);
		}
	}
	
	public void next()
	{
		if (inputTable != null)
		{
			Multimap<Tuple, Tuple> inputTableData = inputTable.getData();
			for (Tuple currentTuple : inputTableData.values())
			{
				cursor.setCurrentTuple(inputTableAlias, currentTuple.toArray());
				boolean isConditionTrue = true;
				for (Expression filterCondition: filterConditions)
					isConditionTrue = isConditionTrue && (Boolean)filterCondition.evaluate(cursor);
				if (isConditionTrue)
					if (nextFilter!=null) nextFilter.next();
			}
		}
	}
	
	public void close()
	{
		if (inputTable != null)
			if (nextFilter!=null) nextFilter.close();
	}

	public String toString()
	{
		StringBuffer s = new StringBuffer("SCAN FILTER\n");
		s.append("  "+inputTableAlias+"\n");
		s.append("  Execution time: ");
		if (nextFilter!=null)
			s.append(executionTime-nextFilter.executionTime);
		else
			s.append(executionTime);
		s.append("\n");
		s.append("  Filter Conditions:"+Arrays.toString(filterConditions)+"\n");
		return s.toString();
	}
}
