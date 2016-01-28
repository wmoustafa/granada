package query.filter;

import java.util.Arrays;

import maputil.Multimap;
import parser.Expression;
import schema.Database;
import schema.Metadata;
import schema.Table;
import schema.Tuple;
import evaluation.TableAlias;

public class ScanFilter extends Filter {

	TableAlias inputTableAlias;
	Table inputTable;

	public ScanFilter(TableAlias inputTableAlias, Expression[] filterConditions) {
		this.inputTableAlias = inputTableAlias;
		this.filterConditions = filterConditions;
	}

	public Filter duplicate() {
		Filter f = new ScanFilter(inputTableAlias, filterConditions);
		if (nextFilter != null)
			f.nextFilter = nextFilter.duplicate();
		return f;
	}

	public void open(Database inputDatabase, Database outputDatabase) {
		inputTable = inputDatabase
				.getDataTableByName(inputTableAlias.tableName);
		if (nextFilter != null) {
			nextFilter.setInputCursor(cursor);
			nextFilter.open(inputDatabase, outputDatabase);
		}
	}

	public void next()
	{
		if (inputTable != null)
		{
			Multimap<Integer, Tuple> inputTableData = inputTable.getData();
			for (Tuple currentTuple : inputTableData.values())
			{
				cursor.setCurrentTuple(inputTableAlias, currentTuple.toArray());
				boolean isConditionTrue = true;
				for (Expression filterCondition: filterConditions)
					isConditionTrue = isConditionTrue && (filterCondition.evaluate(cursor) ==1? true:false);
				if (isConditionTrue)
					if (nextFilter!=null) nextFilter.next();
			}
		}
	}

	public void close() {
		if (inputTable != null)
			if (nextFilter != null)
				nextFilter.close();
	}

	public String toString() {
		StringBuffer s = new StringBuffer("SCAN FILTER\n");
		s.append("  " + inputTableAlias + "\n");
		s.append("  Execution time: ");
		if (nextFilter != null)
			s.append(executionTime - nextFilter.executionTime);
		else
			s.append(executionTime);
		s.append("\n");
		s.append("  Filter Conditions:" + Arrays.toString(filterConditions)
				+ "\n");
		return s.toString();
	}
}
