package schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parser.Expression;

public class Metadata {
	
	class TableMetadata
	{
		int[] keyFields;
		Class[] fieldTypes;

		public TableMetadata(int[] keyFields, Class[] fieldTypes) {
			this.keyFields = keyFields;
			this.fieldTypes = fieldTypes;
		}
		
		public int[] getKeyFields()
		{
			return keyFields;
		}
		
		public Class[] getFieldTypes()
		{
			return fieldTypes;
		}
	}
	
	Map<String, TableMetadata> metadata = new HashMap<String, Metadata.TableMetadata>();
	
	public void setMetadata(String tableName, int[] keyFields, Class[] fieldTypes)
	{
		if (!metadata.containsKey(tableName))
		{
			TableMetadata tableMetadata = new TableMetadata(keyFields, fieldTypes);
			metadata.put(tableName, tableMetadata);
		}
	}
	
	public int[] getKeyFields(String tableName)
	{
		return metadata.get(tableName).getKeyFields();
	}
	
	public Class[] getFieldTypes(String tableName)
	{
		return metadata.get(tableName).getFieldTypes();
	}
	
	public Class[] getTypes(Expression[] outputFields)
	{
		int numberOfOutputFields = outputFields.length;
		Class[] types = new Class[numberOfOutputFields];
		for (int i=0; i<numberOfOutputFields; i++)
			types[i] = outputFields[i].getType(this);
		return types;
	}

}
