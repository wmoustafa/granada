package evaluation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import algebra.RelationalExpression;
import parser.Expression;

public class PossibleOrderSpace {
	
	RelationalExpression relationalExpression;
	Map<Expression, Set<TableAlias>> tableAliasByExpression = new Hashtable<Expression, Set<TableAlias>>();
	Map<TableAlias, Set<TableAlias>> joinGraph = new Hashtable<TableAlias, Set<TableAlias>>();
	List<PossibleOrder> space = new ArrayList<PossibleOrder>();
	
	public PossibleOrderSpace(RelationalExpression relationalExpression)
	{
		this.relationalExpression = relationalExpression;
		createJoinGraph();
		enumeratePossibleOrders();	
	}
	
	private void createJoinGraph()
	{
		for (Expression e : relationalExpression.getConditions())
			tableAliasByExpression.put(e, e.getIncludedTables());
		for	(Expression e : tableAliasByExpression.keySet())
			for (TableAlias tableAlias1 : tableAliasByExpression.get(e))
				for (TableAlias tableAlias2 : tableAliasByExpression.get(e))
				{
					Set<TableAlias> joinedWithTableAlias1 = joinGraph.get(tableAlias1);
					if (joinedWithTableAlias1==null) joinedWithTableAlias1 = new HashSet<TableAlias>(); 
					if (!tableAlias1.equals(tableAlias2)) joinedWithTableAlias1.add(tableAlias2);
					joinGraph.put(tableAlias1, joinedWithTableAlias1);
				}
	}
	
	
	private void enumeratePossibleOrders()
	{
		for (TableAlias tableAlias : relationalExpression.getParticipatingTableAliases())
		{
			PossibleOrder order = new PossibleOrder();
			order.add(tableAlias);
			getNextTableAliasesInOrder(order);
		}
		
	}

	private void getNextTableAliasesInOrder(PossibleOrder order)
	{
		Set<TableAlias> possibleNextOnes = new HashSet<TableAlias>();
		for (TableAlias tableAlias1 : order.getOrdering())
		{
			if (joinGraph.get(tableAlias1)==null) break;
			for (TableAlias tableAlias2 : joinGraph.get(tableAlias1))
				if (!order.contains(tableAlias2))
					possibleNextOnes.add(tableAlias2);
		}
		for (TableAlias tableAlias2 : possibleNextOnes)
		{
			PossibleOrder copy = new PossibleOrder(order);
			copy.add(tableAlias2);
			getNextTableAliasesInOrder(copy);
		}
		if (possibleNextOnes.size()==0)
			space.add(order);
	}
	
	public List<PossibleOrder> getSpace()
	{
		return space;
	}
	
	public Collection<TableAlias> getParticipatingTables()
	{
		return relationalExpression.getParticipatingTableAliases();
	}
	
	
}
