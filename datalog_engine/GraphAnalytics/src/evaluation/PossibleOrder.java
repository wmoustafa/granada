package evaluation;

import java.util.ArrayList;
import java.util.List;

public class PossibleOrder {
	
	private List<TableAlias> ordering;
	
	public PossibleOrder()
	{
		ordering = new ArrayList<TableAlias>();
	}
	
	public PossibleOrder(PossibleOrder p)
	{
		ordering = new ArrayList<TableAlias>();
		for (TableAlias tableAlias : p.ordering)
			ordering.add(tableAlias);
	}
	
	public List<TableAlias> getOrdering()
	{
		return ordering;
	}
	
	public void add(TableAlias tableAlias)
	{
		ordering.add(tableAlias);
	}
	
	public boolean contains(TableAlias tableAlias)
	{
		return ordering.contains(tableAlias);
	}
	
}
