package evaluation;

import java.util.Hashtable;
import java.util.Map;

import main.Main;
import schema.Database;

public class Optimizer {

	Database database;
	PossibleOrderSpace planSpace;
	Map<TableAlias, Integer> aliasTableSizes;
	Map<TableAlias, Integer> tableSizeRanks;
	
	public Optimizer(Database database, PossibleOrderSpace planSpace)
	{
		this.planSpace = planSpace;
		this.database = database;
	}
	
	public Optimizer(Map<String,Integer> tableSizes, PossibleOrderSpace planSpace)
	{
		this.planSpace = planSpace;
	}
	
	void setAliasTableSizes()
	{
		aliasTableSizes = new Hashtable<TableAlias, Integer>();
		for (TableAlias tableAlias : planSpace.getParticipatingTables())
		{
			if (database != null) aliasTableSizes.put(tableAlias, database.getDataTableByName(tableAlias.getTableName()).size());
			else aliasTableSizes.put(tableAlias, 0);
		}
	}
	
	void setTableRanks()
	{
		tableSizeRanks = new Hashtable<TableAlias, Integer>();
		for (TableAlias tableAlias1 : planSpace.getParticipatingTables())
		{
			int rank = 0;
			for (TableAlias tableAlias2 : planSpace.getParticipatingTables())
				if (aliasTableSizes.get(tableAlias1)>=aliasTableSizes.get(tableAlias2)) rank++;
			tableSizeRanks.put(tableAlias1, rank);
		}
	}
	
	public PossibleOrder getBestPlan()
	{
		setAliasTableSizes();
		setTableRanks();
		int minimumError = Integer.MAX_VALUE;
		PossibleOrder bestPlan = new PossibleOrder();
		for (PossibleOrder plan : planSpace.getSpace())
		{
			int planError = 0;
			int i = 0;
			for (TableAlias table : plan.getOrdering())
			{
				i++;
				int error = tableSizeRanks.get(table) - i; 
				planError += (error*error);
			}
			if (planError<minimumError)
			{
				minimumError = planError;
				bestPlan = plan;
			}
		}
		return bestPlan;
	}
	
}
