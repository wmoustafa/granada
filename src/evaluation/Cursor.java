package evaluation;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parser.Expression;
import schema.Tuple;

public class Cursor {

	//Map<TableAlias, Object[]> cursor;
	Object[][] cursor;
	
	public Cursor(int size)
	{
		cursor = new Object[size][];
	}
		
	public void setCurrentTuple(TableAlias tableAlias, Object[] currentTuple)
	{
		cursor[tableAlias.sequenceNumber] = currentTuple;
	}
	
	public Object[] getCurrentTuple(TableAlias tableAlias)
	{
		return cursor[tableAlias.sequenceNumber];
	}
	
	public Object getValue(TableField f)
	{
		//if (f==null) return null;
		return cursor[f.alias.sequenceNumber][f.fieldNumber];
	}
	
	/*public Set<TableAlias> getTableAliases()
	{
		return cursor.keySet();
	}*/
	
	public Tuple evaluate(Expression[] outputFields)
	{
		Object[] values = new Object[outputFields.length];
		int i=0;
		for (Expression e : outputFields)
			values[i++] = e.evaluate(this);
		return new Tuple(values);
	}
		
	public String toString()
	{
		return Arrays.deepToString(cursor);
	}
}
