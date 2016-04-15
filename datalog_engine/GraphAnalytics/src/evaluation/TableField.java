package evaluation;


public class TableField  {
	
	TableAlias alias;
	int fieldNumber;
	
	public TableField(TableAlias alias, int fieldNumber)
	{
		this.alias = alias;
		this.fieldNumber = fieldNumber;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((alias == null) ? 0 : alias.hashCode());
		result = prime * result + fieldNumber;
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
		final TableField other = (TableField) obj;
		if (alias == null) {
			if (other.alias != null)
				return false;
		} else if (!alias.equals(other.alias))
			return false;
		if (fieldNumber != other.fieldNumber)
			return false;
		return true;
	}
	
	
	public TableAlias getAlias()
	{
		return alias;
	}
	
	public int getFieldNumebr()
	{
		return fieldNumber;
	}
	
	public String toString() {
		return alias.toString()+".F"+fieldNumber;		
	}
	

}

