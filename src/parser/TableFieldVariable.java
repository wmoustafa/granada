package parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;
import schema.Database;
import schema.Metadata;

public class TableFieldVariable extends Term {

	TableField field;
	
	public TableFieldVariable(TableField field)
	{
		super();
		this.field = field;
	}
	
	public String toString()
	{
		if (field==null) return null;
		return field.toString();
	}
	
	public Object evaluate(Cursor c)
	{
		return c.getValue(field);
	}

	public Expression substitute(Map<? extends Expression, ? extends Expression> m)
	{
		if (m.get(this)!=null) return m.get(this); else return this;	
	}
	
	public Set<TableAlias> getIncludedTables()
	{
		
		Set<TableAlias> result = new HashSet<TableAlias>();
		result.add(field.getAlias());
		return result;
	}

	public Set<TableField> getIncludedFields()
	{
		Set<TableField> result = new HashSet<TableField>();
		result.add(field);
		return result;
	}

	public Set<DatalogVariable> getIncludedDatalogVariables() {
		return new HashSet<DatalogVariable>();
	}

	public boolean isEquality()
	{
		return false;
	}
	
	public TableField getField()
	{
		return field;
	}

	public Class getType(Database database)
	{
		String tableName = field.getAlias().tableName;
		int fieldNumber = field.getFieldNumebr();
		return database.getDataTableByName(tableName).getFieldTypes()[fieldNumber];
	}

	public Class getType(Metadata metadata)
	{
		String tableName = field.getAlias().tableName;
		int fieldNumber = field.getFieldNumebr();
		return metadata.getFieldTypes(tableName)[fieldNumber];
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((field == null) ? 0 : field.hashCode());
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
		final TableFieldVariable other = (TableFieldVariable) obj;
		if (field == null) {
			if (other.field != null)
				return false;
		} else if (!field.equals(other.field))
			return false;
		return true;
	}

}
