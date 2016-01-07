package parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import schema.Database;
import schema.Metadata;
import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;

public class IntegerConst extends Term {
	
	int value;

	public IntegerConst(int value)
	{
		super ();
		this.value = value;
	}
	
	public String toString()
	{
		return String.valueOf(value);
	}

	@Override
	public int hashCode() {
		return value;
	}

	@Override
	public boolean equals(Object obj) {
		final IntegerConst other = (IntegerConst) obj;
		if (value != other.value)
			return false;
		return true;
	}

	public int evaluate(Cursor m)
	{
		return value;
	}

	public Expression substitute(Map<? extends Expression, ? extends Expression> m)
	{
		return this;	
	}

	public Set<TableAlias> getIncludedTables()
	{
		return new HashSet<TableAlias>();
	}
	
	public Set<TableField> getIncludedFields()
	{
		return new HashSet<TableField>();
	}

	public Set<DatalogVariable> getIncludedDatalogVariables() {
		return new HashSet<DatalogVariable>();
	}

	public boolean isEquality()
	{
		return false;
	}
	
	public Class getType(Database database)
	{
		return Integer.class;
	}

	public Class getType(Metadata metadata)
	{
		return Integer.class;
	}
}
