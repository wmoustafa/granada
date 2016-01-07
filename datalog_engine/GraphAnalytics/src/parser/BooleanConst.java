package parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import schema.Database;
import schema.Metadata;
import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;

public class BooleanConst extends Term {
	
	boolean value;

	public BooleanConst(boolean value)
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
		final int prime = 31;
		int result = 1;
		result = prime * result + (value ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final BooleanConst other = (BooleanConst) obj;
		if (value != other.value)
			return false;
		return true;
	}

	public int evaluate(Cursor m)
	{
		return value?1:0;
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
		return Boolean.class;
	}

	public Class getType(Metadata metadata)
	{
		return Boolean.class;
	}

}
