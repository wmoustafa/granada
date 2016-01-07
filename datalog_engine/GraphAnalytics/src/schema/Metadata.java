package schema;

import java.util.HashMap;
import java.util.Map;

import parser.Expression;
import utils.AggregationFunctionType;
import algebra.RelationalType;

import com.google.common.collect.HashBiMap;

public class Metadata {
	
//	public HashBiMap<String, Integer> name_to_id = HashBiMap.create();
	
	class TableMetadata
	{
		int[] keyFields;
		Class[] fieldTypes;
		boolean isAggregate;
		RelationalType relationalType = RelationalType.NOT_RELATIONAL;
		boolean isRecursive = false;
		boolean isSourceNodeVariableUnncessary = false;
		AggregationFunctionType aggregationFunctionType = AggregationFunctionType.NONE;		

		
		
		public TableMetadata(int[] keyFields, Class[] fieldTypes ) {
			this.keyFields = keyFields;
			this.fieldTypes = fieldTypes;
		}
		
		public TableMetadata(int[] keyFields, Class[] fieldTypes,
				RelationalType relationalType,boolean isRecursive, 
				boolean isSourceNodeVariableUnncessary,AggregationFunctionType aggregationFunctionType  ) {
			this.keyFields = keyFields;
			this.fieldTypes = fieldTypes;
			this.relationalType = relationalType;
			this.isRecursive = isRecursive;
			this.isSourceNodeVariableUnncessary = isSourceNodeVariableUnncessary;
			this.aggregationFunctionType = aggregationFunctionType;
		}
		
		public int[] getKeyFields()
		{
			return keyFields;
		}
		
		public Class[] getFieldTypes()
		{
			return fieldTypes;
		}


		public RelationalType getRelationalType() {
			return relationalType;
		}

		public void setRelationalType(RelationalType relationalType) {
			this.relationalType = relationalType;
		}

		public boolean isRecursive() {
			return isRecursive;
		}

		public void setRecursive(boolean isRecursive) {
			this.isRecursive = isRecursive;
		}

		public boolean isSourceNodeVariableUnncessary() {
			return isSourceNodeVariableUnncessary;
		}

		public void setSourceNodeVariableUnncessary(
				boolean isSourceNodeVariableUnncessary) {
			this.isSourceNodeVariableUnncessary = isSourceNodeVariableUnncessary;
		}

		public AggregationFunctionType getAggregationFunctionType() {
			return aggregationFunctionType;
		}

		public void setAggregationFunctionType(
				AggregationFunctionType aggregationFunctionType) {
			this.aggregationFunctionType = aggregationFunctionType;
		}

		public void setKeyFields(int[] keyFields) {
			this.keyFields = keyFields;
		}

		public void setFieldTypes(Class[] fieldTypes) {
			this.fieldTypes = fieldTypes;
		}
		
		public String toString()
		{
			StringBuilder sb = new StringBuilder();
			
			sb.append("isRecursive=" + isRecursive + 
					", isSourceNodeVariableUnncessary="+ isSourceNodeVariableUnncessary+
					", fieldTypes length="+ fieldTypes.length + "]");
			
			return sb.toString();
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
	
	public void setMetadata(String tableName, int[] keyFields, Class[] fieldTypes,
			RelationalType relationalType,boolean isRecursive, 
			boolean isSourceNodeVariableUnncessary,AggregationFunctionType aggregationFunctionType)
	{
		if (!metadata.containsKey(tableName))
		{
			TableMetadata tableMetadata = new TableMetadata(keyFields, fieldTypes,relationalType,
					isRecursive,isSourceNodeVariableUnncessary,aggregationFunctionType);
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
	


	public RelationalType getRelationalType(String tableName) {
		return  metadata.get(tableName).relationalType;
	}


	public boolean isRecursive(String tableName) {
		return  metadata.get(tableName).isRecursive;
	}


	public boolean isSourceNodeVariableUnncessary(String tableName) {
		return  metadata.get(tableName).isSourceNodeVariableUnncessary;
	}


	public AggregationFunctionType getAggregationFunctionType(String tableName) {
		return metadata.get(tableName).aggregationFunctionType;
	}
	
	
	public Class[] getTypes(Expression[] outputFields)
	{
		int numberOfOutputFields = outputFields.length;
		Class[] types = new Class[numberOfOutputFields];
		for (int i=0; i<numberOfOutputFields; i++)
			types[i] = outputFields[i].getType(this);
		return types;
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for(String name: metadata.keySet()){
			sb.append(name + "[ " + metadata.get(name).toString() + " ]\n");
		}
		return sb.toString();
	}

}
