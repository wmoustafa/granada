package schema;

import giraph.SuperVertexId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import maputil.GoogleMultimap;
import maputil.Multimap;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import parser.IntegerConst;
import utils.AggregationFunctionType;
import utils.PrettyPrinter;
import algebra.RelationalType;



public class Table implements Writable {

	
	private Class[] fieldTypes;
	private int[] keyFields;
	private Multimap<Tuple, Tuple> data;
	private int size=0;
	private boolean isAggregate;
	private RelationalType relationalType = RelationalType.NOT_RELATIONAL;
	private boolean isRecursive = false;
	private boolean isSourceNodeVariableUnncessary = false;
	private AggregationFunctionType aggregationFunctionType = AggregationFunctionType.NONE;
	private static final Logger LOG =
		      Logger.getLogger(Table.class);
	
	/*public Table()
	{
		this.fieldTypes = new Class[0];
		this.keyFields = new int[0];
		this.data = HashMultimap.create();
	}*/

	public Table(Class[] fieldTypes, int[] keyFields)
	{
		this.fieldTypes = fieldTypes;
		this.keyFields = keyFields;
		this.data = new Multimap();
	}
		
	public Table(Class[] fieldTypes, int[] keyFields, int initialSize)
	{
		this.fieldTypes = fieldTypes;
		this.keyFields = keyFields;
		this.data = new Multimap(initialSize);
	}

	public void setAggregate()
	{
		isAggregate = true;
	}
	
	public void setSourceNodeVariableUnncessary()
	{
		isSourceNodeVariableUnncessary = true;
	}

	public boolean isSourceNodeVariableUnncessary()
	{
		return isSourceNodeVariableUnncessary;
	}
	
	public void setAggregationFunctionType(AggregationFunctionType aggregationFunctionType)
	{
		this.aggregationFunctionType = aggregationFunctionType;
		if (aggregationFunctionType == AggregationFunctionType.NONE)
			isAggregate = false;
		else isAggregate = true;
	}
	
	public void setRecursive()
	{
		isRecursive = true;
	}
	
	public boolean isRecursive()
	{
		return isRecursive;
	}
	
	public void setRelationalType(RelationalType relationalType)
	{
		this.relationalType = relationalType;
	}
	
	public boolean isAggregate()
	{
		return isAggregate;
	}
	
	public RelationalType getRelationalType()
	{
		return relationalType;
	}

	public boolean addTuple(Tuple tuple)
	{
		Tuple key = getKey(tuple);
		return addTuple(key, tuple);
	}
	
	public void putTuple(Tuple tuple)
	{
		Tuple key = getKey(tuple);
		data.put(key, tuple);
	}

	public boolean addTuple(Tuple key, Tuple value)
	{
		if (!isAggregate && data.contains(key, value)) return false; 
		if (isAggregate)
		{
			Object[] toBeInserted = value.toArray();
			int aggregateArgIndex = toBeInserted.length - 1;
			for (Tuple t : data.get(key))
			{
				Object[] existing = t.toArray();
				boolean equalGroup = true;
				for (int i = 0; i < aggregateArgIndex; i++)
					if (!toBeInserted[i].equals(existing[i]))
					{
						equalGroup = false;
						break;
					}
				if (equalGroup)
				{
					
					int existingValueBeforeCombining = (Integer)existing[aggregateArgIndex];
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.SUM)
						existing[aggregateArgIndex] = (Integer)toBeInserted[aggregateArgIndex] + (Integer)existing[aggregateArgIndex];
					else if (aggregationFunctionType == AggregationFunctionType.MIN)
						if (((Integer)toBeInserted[aggregateArgIndex]).intValue() < ((Integer) existing[aggregateArgIndex]).intValue()) { existing[aggregateArgIndex]=(Integer)toBeInserted[aggregateArgIndex] ;}
					
					if ((Integer)existing[aggregateArgIndex] == existingValueBeforeCombining) return false; else return true;
					
				}				
			}
		}
		data.put(key, value);
		return true;
	}
	
	public boolean refreshTuple(Table deltaTable, Tuple deltaTuple, LinkedList<Tuple> toBeRemoved)
	{
		Tuple deltaKey = getKey(deltaTuple);
		Tuple deltaValue = deltaTuple;
		if (!isAggregate && data.contains(deltaKey, deltaValue)) { toBeRemoved.add(deltaValue); return false; } 
		if (isAggregate || deltaTable.isAggregate)
		{
			Object[] deltaValueAsArray = deltaValue.toArray();
			int aggregateArgIndex = deltaValueAsArray.length - 1;
			for (Tuple baseValue : data.get(deltaKey))
			{
				Object[] baseValueAsArray = baseValue.toArray();
				boolean equalGroup = true;
				for (int i = 0; i < aggregateArgIndex; i++)
					if (!deltaValueAsArray[i].equals(baseValueAsArray[i]))
					{
						equalGroup = false;
						break;
					}
				if (equalGroup)
				{
					//FOR UPDATING DELTA
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.MIN)
						if (((Integer)deltaValueAsArray[aggregateArgIndex]).intValue() >= ((Integer) baseValueAsArray[aggregateArgIndex]).intValue()){toBeRemoved.add(deltaValue); return false;}

					int existingValueBeforeCombining = (Integer)baseValueAsArray[aggregateArgIndex];
					//FOR UPDATING BASE
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.SUM)
						baseValueAsArray[aggregateArgIndex] = (Integer)deltaValueAsArray[aggregateArgIndex] + (Integer)baseValueAsArray[aggregateArgIndex];
					else if (aggregationFunctionType == AggregationFunctionType.MIN)
						if (((Integer)deltaValueAsArray[aggregateArgIndex]).intValue() < ((Integer)baseValueAsArray[aggregateArgIndex]).intValue()) { baseValueAsArray[aggregateArgIndex] = deltaValueAsArray[aggregateArgIndex]; }
						
					if ((Integer)baseValueAsArray[aggregateArgIndex] == existingValueBeforeCombining) return false; else return true;
				}				
			}
			
		}
		data.put(deltaKey, deltaValue);
		return true;	
	}

	public void diff(Table otherTable, Tuple tuple)
	{
		Tuple key = getKey(tuple);
		Tuple value = tuple;
		if (!isAggregate && otherTable.data.contains(key, value)) { data.remove(key, value); return;} 
		if (isAggregate)
		{
			Object[] toBeInserted = value.toArray();
			int aggregateArgIndex = toBeInserted.length - 1;
			Tuple existingAggregateTuple = null;
			for (Tuple t : otherTable.data.get(key))
			{
				Object[] existing = t.toArray();
				boolean equalGroup = true;
				for (int i = 0; i < aggregateArgIndex; i++)
					if (!toBeInserted[i].equals(existing[i]))
					{
						equalGroup = false;
						break;
					}
				if (equalGroup)
				{
					if (toBeInserted[aggregateArgIndex].equals(existing[aggregateArgIndex])) {data.remove(key, value); return;}
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					// SUM AGGREGATE:
					toBeInserted[aggregateArgIndex] =  (Integer)toBeInserted[aggregateArgIndex] - (Integer)existing[aggregateArgIndex];
					// MIN AGGREGATE: do nothing 
					
					
					break;
				}				
			}
		}
	}

	public Class[] getFieldTypes()
	{
		return fieldTypes;
	}
	
	public int[] getKeyFields()
	{
		return keyFields;
	}
	
	public int size()
	{
		return data.size();
	}
	
	public Multimap<Tuple, Tuple> getData()
	{
		return data;
	}
	
	public Tuple getKey(Tuple value)
	{
		Object[] keys = new Object[keyFields.length];
		Object[] tupleArray = value.toArray();
		int i = 0;
		for (int keyField : keyFields) keys[i++] = tupleArray[keyField];
		Tuple key = new Tuple(keys);
		return key;
	}
	
	public String toString()
	{
		String[][] dataAsMatrix = new String[data.size()][keyFields.length + fieldTypes.length];
		int i = 0;
		for (Tuple tuple : data.values())
		{
			int j = 0;
			for (Object key : getKey(tuple).toArray())
				dataAsMatrix[i][j++] = key.toString();
			for (Object value : tuple.toArray())
				dataAsMatrix[i][j++] = value.toString();
			i++;
		}
		return new PrettyPrinter().toString(dataAsMatrix);
	}
	
	public boolean combine(Table otherTable)
	{
		boolean tableChanged = false;
		for (Tuple tuple : otherTable.data.values())
		{
			boolean tupleChanged = addTuple(tuple);
			if (tupleChanged) tableChanged = true;
		}
		return tableChanged;
	}

	public boolean refresh(Table deltaTable)
	{
		setAggregationFunctionType(deltaTable.aggregationFunctionType);
		setRelationalType(deltaTable.relationalType);
		if (deltaTable.isRecursive) setRecursive();
		if (deltaTable.isSourceNodeVariableUnncessary) setSourceNodeVariableUnncessary();
		
		boolean tableChanged = false;
		LinkedList<Tuple> toBeRemoved = new LinkedList<Tuple>();
		for (Tuple tuple : deltaTable.data.values())
		{
			boolean tupleChanged = refreshTuple(deltaTable, tuple, toBeRemoved);
			if (tupleChanged) tableChanged = true;
		}
		for (Tuple tuple : toBeRemoved)
			deltaTable.data.remove(getKey(tuple), tuple);
		return tableChanged;
	}


	public boolean isEmpty()
	{
		return data.size() == 0;
	}
	
	public Map<SuperVertexId,Table> partition(Table neighborsTable, Table neighborsSuperVerticesTable)
	{
		Map<SuperVertexId,Table> partitionedTable = new HashMap<SuperVertexId, Table>();
		for (Tuple value : data.values())
		{
			Tuple key = getKey(value);

			Collection<Tuple> neighbors = neighborsTable.data.get(key);
			for (Tuple n : neighbors)
			{
			int neighborId = (Integer)n.toArray()[1];
			Tuple neighborsSuperVerticesKey = new Tuple(new Object[]{neighborId});
			Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			Object[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
			SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexArray[1])).shortValue(), (Integer)(neighborSuperVertexArray[2]));
			Table existingTable = partitionedTable.get(neighborSuperVertexId);
			if (existingTable == null)
			{
				existingTable = new Table(fieldTypes, keyFields);
				existingTable.setAggregationFunctionType(aggregationFunctionType);
				if (isRecursive) existingTable.setRecursive();
				if (isSourceNodeVariableUnncessary) existingTable.setSourceNodeVariableUnncessary();
				existingTable.setRelationalType(relationalType);
				partitionedTable.put(neighborSuperVertexId, existingTable);
			}
			if (!existingTable.data.contains(key, value)) existingTable.data.put(key, value);
			}
		}
		return partitionedTable;
	}
	
	public Map<SuperVertexId,Table> partitionUseSemiJoin(Table neighborsSuperVerticesTable)
	{
		Map<SuperVertexId,Table> partitionedTable = new HashMap<SuperVertexId, Table>();
		// The line below should change if source node index changes
		// somehow in the code that rewrites the rule
		// Right now, it is guaranteed to be at 0
		int sourceNodeIdIndex = 0;

		int neighborIdIndex;
		if (isAggregate) neighborIdIndex = fieldTypes.length - 2;
		else neighborIdIndex = fieldTypes.length - 1;

		for (Tuple value : data.values())
		{
			Tuple key = getKey(value);
			Object[] tupleArray = value.toArray();

			int neighborId = (Integer)tupleArray[neighborIdIndex];
			Tuple neighborsSuperVerticesKey = new Tuple(new Object[]{neighborId});
			Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			Object[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
			SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexArray[1])).shortValue(), (Integer)(neighborSuperVertexArray[2]));
			Table existingTable = partitionedTable.get(neighborSuperVertexId);
			if (existingTable == null)
			{
				existingTable = new Table(fieldTypes, keyFields);
				existingTable.setAggregationFunctionType(aggregationFunctionType);
				if (isRecursive) existingTable.setRecursive();
				if (isSourceNodeVariableUnncessary) existingTable.setSourceNodeVariableUnncessary();
				existingTable.setRelationalType(relationalType);
				partitionedTable.put(neighborSuperVertexId, existingTable);
			}
			//if (!existingTable.data.contains(key, value))
			{
				if (isSourceNodeVariableUnncessary) tupleArray[sourceNodeIdIndex] = 0;
				//existingTable.data.put(key, value);
				existingTable.addTuple(key, value);
			}
		}
		return partitionedTable;
	}

	
	public Map<SuperVertexId,PartitionWithMessages> partitionUseSemiAsync(Table neighborsTable, Table neighborsSuperVerticesTable, Table messagesTable, Table otherDirectionNeighborsTable, boolean isPagerank)
	{
		Map<SuperVertexId,PartitionWithMessages> partitionedTableWithMessages = new HashMap<SuperVertexId, PartitionWithMessages>();		// The line below should change if source node index changes

		for (Tuple value : data.values())
		{
			Tuple key = getKey(value);

			int iterationNumber = 1;

			//For PageRank
			iterationNumber = (Integer)(value.toArray()[1]);
			
			int numberOfRecievedMessages = getNumberOfMessages(key, iterationNumber - 1, messagesTable);
			int numberOfOtherSideNeighbors = getNumberOfNeighbors(key, otherDirectionNeighborsTable);
			if (isPagerank)
				{ if (isRecursive && iterationNumber > 0 && numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}
			else
				{ if (numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}

			Collection<Tuple> neighbors = neighborsTable.data.get(key);
			for (Tuple n : neighbors)
			{
				int neighborId = (Integer)n.toArray()[1];
				Tuple neighborsSuperVerticesKey = new Tuple(new Object[]{neighborId});
				Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
				Object[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
				SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexArray[1])).shortValue(), (Integer)(neighborSuperVertexArray[2]));
				PartitionWithMessages existingPartitionWithMessages = partitionedTableWithMessages.get(neighborSuperVertexId);
				if (existingPartitionWithMessages == null)
				{
					Table partition = new Table(fieldTypes, keyFields);
					partition.setAggregationFunctionType(aggregationFunctionType);
					if (isRecursive) partition.setRecursive();
					if (isSourceNodeVariableUnncessary) partition.setSourceNodeVariableUnncessary();
					partition.setRelationalType(relationalType);
					
					Table neighborMessagesTable = createMessageTable();
					
					existingPartitionWithMessages = new PartitionWithMessages(partition, neighborMessagesTable);
					partitionedTableWithMessages.put(neighborSuperVertexId, existingPartitionWithMessages);
				}
				if (!existingPartitionWithMessages.partition.data.contains(key, value)) existingPartitionWithMessages.partition.data.put(key, value);
				addSingleMessage(neighborId, iterationNumber, existingPartitionWithMessages.messages);
			}
			//resetNumberOfMessages(key, iterationNumber, messagesTable);
		}
		return partitionedTableWithMessages;
	}
	
	public Map<SuperVertexId,PartitionWithMessages> partitionUseSemiJoinSemiAsync(Table neighborsSuperVerticesTable, Table messagesTable, Table otherDirectionNeighborsTable, boolean isPagerank)
	{
		Map<SuperVertexId,PartitionWithMessages> partitionedTableWithMessages = new HashMap<SuperVertexId, PartitionWithMessages>();
		// The line below should change if source node index changes
		// somehow in the code that rewrites the rule
		// Right now, it is guaranteed to be at 0
		int sourceNodeIdIndex = 0;
		
		int neighborIdIndex;
		if (isAggregate) 
			neighborIdIndex = fieldTypes.length - 2;
		else neighborIdIndex = fieldTypes.length - 1;
		
		Set<Tuple> processedSourceNodeIdAndIterationNumberTuples = new HashSet<>();
		for (Tuple value : data.values())
		{
			Object[] tupleArray = value.toArray();

			Tuple destNodeIdTuple = getKey(value);

			int iterationNumber = 1;

			//For PageRank
			iterationNumber = (Integer)(value.toArray()[1]);

			Tuple sourceNodeIdTuple = new Tuple(new Object[]{tupleArray[sourceNodeIdIndex]});
			Tuple sourceNodeIdAndIterationNumberTuple = new Tuple(new Object[]{tupleArray[sourceNodeIdIndex], iterationNumber});

			int numberOfRecievedMessages = getNumberOfMessages(sourceNodeIdTuple, iterationNumber - 1, messagesTable);
			int numberOfOtherSideNeighbors = getNumberOfNeighbors(sourceNodeIdTuple, otherDirectionNeighborsTable);
			
			if (isPagerank)
				{ if (isRecursive && iterationNumber > 0 && numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}
			
			else
				{ if (numberOfRecievedMessages != numberOfOtherSideNeighbors) continue; }

			int neighborId = (Integer)tupleArray[neighborIdIndex];
			Tuple neighborsSuperVerticesKey = new Tuple(new Object[]{neighborId});
			Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			Object[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
			SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexArray[1])).shortValue(), (Integer)(neighborSuperVertexArray[2]));
			PartitionWithMessages existingPartitionWithMessages = partitionedTableWithMessages.get(neighborSuperVertexId);
			if (existingPartitionWithMessages == null)
			{
				Table partition = new Table(fieldTypes, keyFields);
				partition.setAggregationFunctionType(aggregationFunctionType);
				if (isRecursive) partition.setRecursive();
				if (isSourceNodeVariableUnncessary) partition.setSourceNodeVariableUnncessary();
				partition.setRelationalType(relationalType);
				
				Table neighborMessagesTable = createMessageTable();
				
				existingPartitionWithMessages = new PartitionWithMessages(partition, neighborMessagesTable);
				partitionedTableWithMessages.put(neighborSuperVertexId, existingPartitionWithMessages);
			}
			if (!existingPartitionWithMessages.partition.data.contains(destNodeIdTuple, value))
			{
				if (isSourceNodeVariableUnncessary) tupleArray[sourceNodeIdIndex] = 0;
				existingPartitionWithMessages.partition.addTuple(destNodeIdTuple, value);
			}
			addSingleMessage(neighborId, iterationNumber, existingPartitionWithMessages.messages);
			
			processedSourceNodeIdAndIterationNumberTuples.add(sourceNodeIdAndIterationNumberTuple);
		}
		return partitionedTableWithMessages;
	}

	Table createMessageTable()
	{
		int[] messagesKeyFields = new int[]{0};
		Class[] messagesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
		Table messagesTable = new Table(messagesFieldTypes, messagesKeyFields);
		messagesTable.setAggregationFunctionType(AggregationFunctionType.SUM);
		messagesTable.setAggregate();	
		return messagesTable;
	}
	
	void addSingleMessage(int destinationVertexId, int iterationNumber, Table messagesTable)
	{
		Object[] messagesTuple = new Object[3];
		messagesTuple[0] = destinationVertexId;
		messagesTuple[1] = iterationNumber;
		messagesTuple[2] = 1;
		messagesTable.addTuple(new Tuple(messagesTuple));		
	}
	
	class PartitionWithMessages
	{
		Table partition;
		Table messages;
		public PartitionWithMessages(Table partition, Table messages) {
			super();
			this.partition = partition;
			this.messages = messages;
		}
		
	}

	int getNumberOfMessages(Tuple key, int iterationNumber, Table messagesTable)
	{
		for (Tuple messageTuple : messagesTable.data.get(key))
		{
			Object[] messageTupleArray = messageTuple.toArray();
			if ((Integer)messageTupleArray[1] == iterationNumber) return (Integer)messageTupleArray[2];
		}
		return 0;
	}
	
	void resetNumberOfMessages(Tuple key, int iterationNumber, Table messagesTable)
	{
		Tuple toDelete = null;
		for (Tuple messageTuple : messagesTable.data.get(key))
		{
			Object[] messageTupleArray = messageTuple.toArray();
			if ((Integer)messageTupleArray[1] == iterationNumber) toDelete = messageTuple;
		}
		if (toDelete != null) messagesTable.data.remove(key, toDelete);
	}

	
int getNumberOfNeighbors(Tuple key, Table neighborsTable)
	{
		int numberOfNeighbors = 0;
		for (Tuple t : neighborsTable.data.get(key))
				numberOfNeighbors++;
		return numberOfNeighbors;
	}

	public void readFieldsOriginal(DataInput in) throws IOException {
		int nFieldTypes = WritableUtils.readVInt(in);
		isAggregate = WritableUtils.readVInt(in) == 1;
		fieldTypes = new Class[nFieldTypes];
		for (int i = 0; i < nFieldTypes; i++)
		{
			int fieldType = WritableUtils.readVInt(in);
			if (fieldType == 0) fieldTypes[i] = String.class;
			else if (fieldType == 1) fieldTypes[i] = Integer.class;
			else if (fieldType == 2) fieldTypes[i] = Boolean.class;
		}

		int nKeyFields = WritableUtils.readVInt(in);
		keyFields = new int[nKeyFields];
		for (int i = 0; i < nKeyFields; i++)
			keyFields[i] = WritableUtils.readVInt(in);

		size = WritableUtils.readVInt(in);
		for (int k = 0; k < size; k++)
		{
			Object[] array = new Object[fieldTypes.length];
			for (int i = 0; i < fieldTypes.length; i++)
			{
				if (fieldTypes[i] == String.class) array[i] = WritableUtils.readCompressedString(in);
				else if (fieldTypes[i] == Integer.class) array[i] = WritableUtils.readVInt(in);
				else if (fieldTypes[i] == Boolean.class) array[i] = (WritableUtils.readVInt(in) == 1);
			}
			Tuple tuple = new Tuple(array);
			addTuple(tuple);
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int nFieldTypes = in.readInt();
		isRecursive = in.readBoolean();
		isSourceNodeVariableUnncessary = in.readBoolean();
		relationalType = RelationalType.values()[in.readByte()];
		setAggregationFunctionType(AggregationFunctionType.values()[in.readByte()]);
		fieldTypes = new Class[nFieldTypes];
		for (int i = 0; i < nFieldTypes; i++)
		{
			byte fieldType = in.readByte();
			if (fieldType == 1) fieldTypes[i] = Integer.class;
			else if (fieldType == 0) fieldTypes[i] = String.class;
			else if (fieldType == 2) fieldTypes[i] = Boolean.class;
		}

		int nKeyFields = in.readInt();
		keyFields = new int[nKeyFields];
		for (int i = 0; i < nKeyFields; i++)
			keyFields[i] = in.readInt();

		size = in.readInt();
		data = new Multimap<>();
		if (isSourceNodeVariableUnncessary)
		{
			for (int k = 0; k < size; k++)
			{
				Object[] array = new Object[fieldTypes.length];
				array[0] = 0;
				for (int i = 1; i < fieldTypes.length; i++)
				{
					if (fieldTypes[i] == Integer.class) array[i] = in.readInt();
					else if (fieldTypes[i] == String.class) array[i] = in.readUTF();
					else if (fieldTypes[i] == Boolean.class) array[i] = in.readBoolean();
				}
				Tuple tuple = new Tuple(array);
				addTuple(tuple);
			}
		}
		else	
			for (int k = 0; k < size; k++)
			{
				Object[] array = new Object[fieldTypes.length];
				for (int i = 0; i < fieldTypes.length; i++)
				{
					if (fieldTypes[i] == Integer.class) array[i] = in.readInt();
					else if (fieldTypes[i] == String.class) array[i] = in.readUTF();
					else if (fieldTypes[i] == Boolean.class) array[i] = in.readBoolean();
				}
				Tuple tuple = new Tuple(array);
				addTuple(tuple);
			}
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fieldTypes.length);
		out.writeBoolean(isRecursive);
		out.writeBoolean(isSourceNodeVariableUnncessary);		
		out.writeByte(relationalType.ordinal());
		out.writeByte(aggregationFunctionType.ordinal());
		for (Class fieldType : fieldTypes)
			if (fieldType == Integer.class) out.writeByte(1);
			else if (fieldType == String.class) out.writeByte(0);
			else if (fieldType == Boolean.class) out.writeByte(2);

		out.writeInt( keyFields.length);
		for (int keyField : keyFields)
			out.writeInt(keyField);
		
		out.writeInt(data.size());
		if (isSourceNodeVariableUnncessary)
		{
			for (Tuple tuple : data.values())
			{
				Object[] array = tuple.toArray(); 
				for (int i = 1; i < array.length; i++)
				{
					Object value = array[i];
					if (fieldTypes[i] == Integer.class) out.writeInt((Integer) value);
					else if (fieldTypes[i] == String.class) out.writeUTF((String) value);
					else if (fieldTypes[i] == Boolean.class) out.writeBoolean((Boolean) value);
				}
			}
		}
		else
		{
			for (Tuple tuple : data.values())
			{
				Object[] array = tuple.toArray(); 
				for (int i = 0; i < array.length; i++)
				{
					Object value = array[i];
					if (fieldTypes[i] == Integer.class) out.writeInt((Integer) value);
					else if (fieldTypes[i] == String.class) out.writeUTF((String) value);
					else if (fieldTypes[i] == Boolean.class) out.writeBoolean((Boolean) value);
				}
			}
		}
		
	}
}
