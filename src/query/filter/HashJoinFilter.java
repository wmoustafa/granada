package query.filter;

import java.util.Arrays;

import evaluation.Cursor;
import evaluation.TableAlias;
import maputil.Multimap;
import parser.Expression;
import schema.Database;
import schema.Metadata;
import schema.Table;
import schema.Tuple;
	
public class HashJoinFilter extends Filter {

	boolean buildRightHashTable;
	Multimap<Integer,Tuple> rightBuild;
	Expression[] lhsExpressions, rhsExpressions;
	TableAlias rhsTableAlias;
	
	public HashJoinFilter(TableAlias rhsTableAlias, Expression[] lhsExpressions, Expression[] rhsExpressions, Expression[] filterConditions, boolean buildRightHashTable)
	{
		assert(lhsExpressions.length == 1);
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
	
	public void open(Database inputDatabase, Database outputDatabase, Metadata metadata)
	{
		if (nextFilter!=null) 
		{
			nextFilter.setInputCursor(cursor);
			nextFilter.open(inputDatabase, outputDatabase, metadata);
		}
		Table rhsDataTable = inputDatabase.getDataTableByName(rhsTableAlias.tableName);
//		System.out.println("rhsTableAlias.tableName = " + rhsTableAlias.tableName);
//		System.out.println("HashJoin right table = " + rhsDataTable);
		//if (rhsDataTable==null) { //System.out.println("^^^^^^^^^^^^"+rhsTableAlias.tableName); System.out.println("");}
		
		if (rhsDataTable != null) {
			
			if (buildRightHashTable && rhsExpressions.length != 0)
			{
//				System.out.println("Creating hash index for Join");
				results = rhsDataTable.getData().values().iterator();		
				Cursor rhsCursor = new Cursor(rhsTableAlias.sequenceNumber + 1);
				rightBuild = new Multimap(); 
				while (results.hasNext())
				{
					Tuple currentTuple = results.next();
					rhsCursor.setCurrentTuple(rhsTableAlias, currentTuple.toArray());
					int v = rhsCursor.evaluate(rhsExpressions).toArray()[0];
					rightBuild.put(v, currentTuple);
				}
			}
			else rightBuild = rhsDataTable.getData();
		}
	}
	
	public void next()
	{
		if (rightBuild != null)
		{
			//int lhsValues = cursor.evaluate(lhsExpressions).toArray()[0];
			int lhsValues = lhsExpressions[0].evaluate(cursor);

//			System.out.println("left tuples = " + lhsValues);
			Iterable<Tuple> matchingTuples;
			if (rhsExpressions.length != 0)
				matchingTuples = rightBuild.get(lhsValues);
			else
				matchingTuples = rightBuild.values();
			if (matchingTuples!=null)
			{
				for (Tuple currentTuple : matchingTuples)
				{
					cursor.setCurrentTuple(rhsTableAlias, currentTuple.toArray());
					boolean isConditionTrue = true;
					for (Expression filterCondition: filterConditions)
						if (filterCondition.evaluate(cursor) == 1?false:true) {
							isConditionTrue = false; 
							break;
						}
					if (isConditionTrue)
						if (nextFilter!=null) nextFilter.next();
				}
				//Log.DEBUG(numberOfJoiningTuples+","+m+","+(float)numberOfJoiningTuples/m);
				matchingTuples = null;
			}
		}		
	}
	public void close()
	{
//		System.out.println("***************************************************");
//		System.out.println(this);
		//DatabaseUtils.printDatabase(rhsTableAlias.databaseName);
		if (rightBuild != null) 
			if (nextFilter!=null) nextFilter.close();
	}
	
	public String toString()
	{
		StringBuffer s = new StringBuffer("HASH JOIN FILTER\n");
		s.append("  "+rhsTableAlias+"\n");
		s.append("  Execution time: ");
//		if (nextFilter!=null)
//		{
//			s.append(executionTime-nextFilter.executionTime);
//			s.append(" "+(openExecutionTime-nextFilter.openExecutionTime)+"+");
//			s.append((nextExecutionTime-nextFilter.nextExecutionTime)+"+");
//			s.append((closeExecutionTime-nextFilter.closeExecutionTime));
//		}
//		else
//		{
//			s.append(executionTime);
//			s.append(" "+(openExecutionTime)+"+");
//			s.append((nextExecutionTime)+"+");
//			s.append((closeExecutionTime));
//		}
		s.append("\n");
		s.append("	BuildRighHashTable: " + buildRightHashTable + "\n");
		s.append("  Left Expressions:"+Arrays.toString(lhsExpressions)+"\n");
		s.append("  Right Expressions:"+Arrays.toString(rhsExpressions)+"\n");
		s.append("  Filter Conditions:"+Arrays.toString(filterConditions)+"\n");
		return s.toString();
	}

}
