package parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import schema.Database;
import schema.Metadata;
import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;

public class DatalogVariable extends Term {
	
	String name;
	
	public DatalogVariable(String name)
	{
		super();
		this.name = name;
	}
	
	public String toString()
	{
		return name;
	}
	
	public int evaluate(Cursor c)
	{
		//throw new Exception
		return -1;
	}

	public Expression substitute(Map<? extends Expression, ? extends Expression> m)
	{
		if (m.get(this)!=null) return m.get(this); else return this;
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
		Set<DatalogVariable> datalogVariables = new HashSet<>();
		if (!name.startsWith("DontCare")) datalogVariables.add(this);
		return datalogVariables;
	}

	public boolean isEquality()
	{
		return false;
	}

	public Class getType(Database database)
	{
		return null;
	}

	public Class getType(Metadata metadata)
	{
		return null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		final DatalogVariable other = (DatalogVariable) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
