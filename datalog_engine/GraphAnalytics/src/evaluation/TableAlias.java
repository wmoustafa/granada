package evaluation;

public class TableAlias {
	
	public String tableName;
	public int sequenceNumber;
	
	public TableAlias(String tableName, int sequenceNumber)
	{
		this.tableName = tableName;
		this.sequenceNumber = sequenceNumber;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(int sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	@Override
	public boolean equals(Object obj) {
		final TableAlias other = (TableAlias) obj;
		if (!tableName.equals(other.tableName))
			return false;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		return true;
	}
	
	@Override
	public int hashCode() {
		return tableName.hashCode()+sequenceNumber;
		/*final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((databaseName == null) ? 0 : databaseName.hashCode());
		result = prime * result + sequenceNumber;
		return result;*/
	}
	
	public String toString()
	{
		return tableName+"_"+sequenceNumber;
	}

}
