package parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;
import schema.Database;
import schema.Metadata;

public class StringConst extends Term {

	String value;

	public StringConst(String value)
	{
		super ();
		this.value = value;
	}
	
	public String toString()
	{
		return "'"+value+"'";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		final StringConst other = (StringConst) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public int evaluate(Cursor m)
	{
		throw new RuntimeException("Unsupported operation");
		//return value;
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
		return String.class;
	}

	public Class getType(Metadata metadata)
	{
		return String.class;
	}
}
