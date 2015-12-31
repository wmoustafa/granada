package query.filter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import maputil.GoogleMultimap;
import maputil.Multimap;
import parser.Expression;
import evaluation.Cursor;
import evaluation.TableAlias;
import schema.Table;
import schema.Database;
import schema.Tuple;
	
public class HashJoinFilter extends Filter {

	boolean buildRightHashTable;
	Multimap<Tuple,Tuple> rightBuild;
	Expression[] lhsExpressions, rhsExpressions;
	TableAlias rhsTableAlias;
	
	public HashJoinFilter(TableAlias rhsTableAlias, Expression[] lhsExpressions, Expression[] rhsExpressions, Expression[] filterConditions, boolean buildRightHashTable)
	{
		this.rhsTableAlias = rhsTableAlias;
		this.filterConditions = filterConditions;
		this.lhsExpressions = lhsExpressions;
		this.rhsExpressions = rhsExpressions;
		this.buildRightHashTable = buildRightHashTable;
	}
		
	public Filter duplicate()
	{
		HashJoinFilter f = new HashJoinFilter(rhsTableAlias, lhsExpressions, rhsExpressions, filterConditions, buildRightHashTable);
		if (nextFilter != null) f.nextFilter = nextFilter.duplicate();
		return f;
	}
	
	public void open(Database inputDatabase, Database outputDatabase)
	{
		if (nextFilter!=null) 
		{
			nextFilter.setInputCursor(cursor);
			nextFilter.open(inputDatabase, outputDatabase);
		}
		Table rhsDataTable = inputDatabase.getDataTableByName(rhsTableAlias.tableName);
		
		if (rhsDataTable != null)
			if (buildRightHashTable && rhsExpressions.length != 0)
			{
				results = rhsDataTable.getData().values().iterator();		
				Cursor rhsCursor = new Cursor(rhsTableAlias.sequenceNumber + 1);
				rightBuild = new Multimap(); 
				while (results.hasNext())
				{
					Tuple currentTuple = results.next();
					rhsCursor.setCurrentTuple(rhsTableAlias, currentTuple.toArray());
					Tuple v = rhsCursor.evaluate(rhsExpressions);
					rightBuild.put(v, currentTuple);
				}
			}
			else rightBuild = rhsDataTable.getData();
	}
	
	public void next()
	{
		if (rightBuild != null)
		{
			Tuple lhsValues = cursor.evaluate(lhsExpressions);
			Iterable<Tuple> matchingTuples;
			if (rhsExpressions.length == 0)
				matchingTuples = rightBuild.values();
			else
				matchingTuples = rightBuild.get(lhsValues);
			if (matchingTuples!=null)
			{
				for (Tuple currentTuple : matchingTuples)
				{
					cursor.setCurrentTuple(rhsTableAlias, currentTuple.toArray());
					boolean isConditionTrue = true;
					for (Expression filterCondition: filterConditions)
						if (!(Boolean)filterCondition.evaluate(cursor)) {
							isConditionTrue = false; 
							break;
						}
					if (isConditionTrue)
						if (nextFilter!=null) nextFilter.next();
				}
				//Log.DEBUG(numberOfJoiningTuples+","+m+","+(float)numberOfJoiningTuples/m);
			}
		}		
	}
	public void close()
	{
		if (rightBuild != null) 
			if (nextFilter!=null) nextFilter.close();
	}
	
	public String toString()
	{
		StringBuffer s = new StringBuffer("HASH JOIN FILTER\n");
		s.append("  "+rhsTableAlias+"\n");
		s.append("  Execution time: ");
		if (nextFilter!=null)
		{
			s.append(executionTime-nextFilter.executionTime);
			s.append(" "+(openExecutionTime-nextFilter.openExecutionTime)+"+");
			s.append((nextExecutionTime-nextFilter.nextExecutionTime)+"+");
			s.append((closeExecutionTime-nextFilter.closeExecutionTime));
		}
		else
		{
			s.append(executionTime);
			s.append(" "+(openExecutionTime)+"+");
			s.append((nextExecutionTime)+"+");
			s.append((closeExecutionTime));
		}
		s.append("\n");
		s.append("  Left Expressions:"+Arrays.toString(lhsExpressions)+"\n");
		s.append("  Right Expressions:"+Arrays.toString(rhsExpressions)+"\n");
		s.append("  Filter Conditions:"+Arrays.toString(filterConditions)+"\n");
		return s.toString();
	}

}
