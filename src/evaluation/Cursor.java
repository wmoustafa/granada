package evaluation;

import java.util.Arrays;

import parser.Expression;
import schema.Tuple;

public class Cursor {

	//Map<TableAlias, Object[]> cursor;
	int[][] cursor;
	
	public Cursor(int size)
	{
		cursor = new int[size][];
	}
		
	public void setCurrentTuple(TableAlias tableAlias, int[] currentTuple)
	{
		cursor[tableAlias.sequenceNumber] = currentTuple;
	}
	
	public int[] getCurrentTuple(TableAlias tableAlias)
	{
		return cursor[tableAlias.sequenceNumber];
	}
	
	public int getValue(TableField f)
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
//		return outputFields[0].evaluate(this);
		int[] values = new int[outputFields.length];
		int i=0;
		for (Expression e : outputFields)
		{
//			System.out.println("Expression = " + e.toString());
			values[i++] = e.evaluate(this);
		}
//		System.out.println("Join creates new tuple");
		return new Tuple(values);
	}
		
	public String toString()
	{
		return Arrays.deepToString(cursor);
	}
}
