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

	public enum edb_columns { 
		VERTICES(2), EDGES(3), NEIGHBORS(3), MESSAGES(3);
		
		private int value;
		private edb_columns(int value) { this.value = value; }
		
	}
	
	private String name;
	private Class[] fieldTypes;
	private int[] keyFields;
	private Multimap<Integer, Tuple> data;
	private int size=0;
	private boolean isAggregate;
	private RelationalType relationalType = RelationalType.NOT_RELATIONAL;
	private boolean isRecursive = false;
	private boolean isSourceNodeVariableUnncessary = false;
	private AggregationFunctionType aggregationFunctionType = AggregationFunctionType.NONE;
	private static final Logger LOG =  Logger.getLogger(Table.class);
	private edb_columns type;
	private Metadata metadata;

	
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
	
	public Table(String name, Class[] fieldTypes, int[] keyFields, Metadata metadata)
	{
		this.name = name;
		this.fieldTypes = fieldTypes;
		this.keyFields = keyFields;
		this.metadata = metadata;
		this.data = new Multimap();
	}
	
	public Table(String name, Class[] fieldTypes, int[] keyFields, int initialSize, Metadata metadata)
	{
		this.name = name;
		this.fieldTypes = fieldTypes;
		this.keyFields = keyFields;
		this.metadata = metadata;
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
	
	public void setType(edb_columns c) {
		this.type = c;
	}
	
	public RelationalType getRelationalType()
	{
		return relationalType;
	}

	public boolean addTuple(Tuple tuple)
	{
		int key = getKey(tuple);
		return addTuple(key, tuple);
	}
	
	public void putTuple(Tuple tuple)
	{
		int key = getKey(tuple);
		data.put(key, tuple);
	}

	public boolean addTuple(int key, Tuple value)
	{
		if (!isAggregate && data.contains(key, value)) return false; 
		if (isAggregate)
		{
			int[] toBeInserted = value.toArray();
			int aggregateArgIndex = toBeInserted.length - 1;
			for (Tuple t : data.get(key))
			{
				int[] existing = t.toArray();
				boolean equalGroup = true;
				for (int i = 0; i < aggregateArgIndex; i++)
					if (toBeInserted[i] != (existing[i]))
					{
						equalGroup = false;
						break;
					}
				if (equalGroup)
				{					
					int existingValueBeforeCombining = (Integer)existing[aggregateArgIndex];
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.SUM){
						existing[aggregateArgIndex] = toBeInserted[aggregateArgIndex] + 
						existing[aggregateArgIndex];
					}
					else if (aggregationFunctionType == AggregationFunctionType.MIN) {
						if (toBeInserted[aggregateArgIndex] < existing[aggregateArgIndex]) {
							existing[aggregateArgIndex]= toBeInserted[aggregateArgIndex];
						}
					}
					
					if (existing[aggregateArgIndex] == existingValueBeforeCombining) 
						return false; 
					else return true;
					
				}				
			}
		}
		data.put(key, value);
		return true;
	}
	
	public boolean addAndSubtractTuple(Table deltaTable, Tuple deltaTuple, LinkedList<Tuple> toBeRemoved)
	{
		int deltaKey = getKey(deltaTuple);
		Tuple deltaValue = deltaTuple;
//		System.out.println("base table is aggregate:" + isAggregate);
//		System.out.println("delta table is aggregate:" + deltaTable.isAggregate);
		if (!isAggregate && data.contains(deltaKey, deltaValue)) 
		{ 
			toBeRemoved.add(deltaValue); 
			return false; 
		} 
		if (isAggregate || deltaTable.isAggregate)
		{
			int[] deltaValueAsArray = deltaValue.toArray();
			int aggregateArgIndex = deltaValueAsArray.length - 1;
			for (Tuple baseValue : data.get(deltaKey))
			{
				int[] baseValueAsArray = baseValue.toArray();
				boolean equalGroup = true;
				for (int i = 0; i < aggregateArgIndex; i++)
					if (deltaValueAsArray[i] != (baseValueAsArray[i]))
					{
						equalGroup = false;
						break;
					}
				if (equalGroup)
				{				
					//FOR UPDATING DELTA
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.MIN)
						if (((Integer)deltaValueAsArray[aggregateArgIndex]).intValue() >= 
						((Integer) baseValueAsArray[aggregateArgIndex]).intValue())
						{
							toBeRemoved.add(deltaValue); 
							return false;
						}

					int existingValueBeforeCombining = (Integer)baseValueAsArray[aggregateArgIndex];
					//FOR UPDATING BASE
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.SUM)
						baseValueAsArray[aggregateArgIndex] = (Integer)deltaValueAsArray[aggregateArgIndex] +
						(Integer)baseValueAsArray[aggregateArgIndex];
					else if (aggregationFunctionType == AggregationFunctionType.MIN)
						if (((Integer)deltaValueAsArray[aggregateArgIndex]).intValue() < 
								((Integer)baseValueAsArray[aggregateArgIndex]).intValue()) 
						{
							baseValueAsArray[aggregateArgIndex] = deltaValueAsArray[aggregateArgIndex]; 
						}
						
					if ((Integer)baseValueAsArray[aggregateArgIndex] == existingValueBeforeCombining) 
						return false; 
					else return true;
				}				
			}			
		}
		data.put(deltaKey, deltaValue);
		return true;	
	}

	public void diff(Table otherTable, Tuple tuple)
	{
		int key = getKey(tuple);
		Tuple value = tuple;
		if (!isAggregate && otherTable.data.contains(key, value)) { data.remove(key, value); return;} 
		if (isAggregate)
		{
			int[] toBeInserted = value.toArray();
			int aggregateArgIndex = toBeInserted.length - 1;
			Tuple existingAggregateTuple = null;
			for (Tuple t : otherTable.data.get(key))
			{
				int[] existing = t.toArray();
				boolean equalGroup = true;
				for (int i = 0; i < aggregateArgIndex; i++)
					if (toBeInserted[i] == (existing[i]))
					{
						equalGroup = false;
						break;
					}
				if (equalGroup)
				{
					if (toBeInserted[aggregateArgIndex] == (existing[aggregateArgIndex])) {
						data.remove(key, value); 
						return;
					}
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					// SUM AGGREGATE:
					toBeInserted[aggregateArgIndex] =  (Integer)toBeInserted[aggregateArgIndex] - 
							(Integer)existing[aggregateArgIndex];
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
	
	public Multimap<Integer, Tuple> getData()
	{
		return data;
	}
	
	public int getKey(Tuple value)
	{
		//assert(keyFields.length == 1);
		
		//int[] keys = new int[keyFields.length];
		//int[] tupleArray = value.toArray();
		
//		int i = 0;
//		for (int keyField : keyFields) keys[i++] = tupleArray[keyField];
//		Tuple key = new Tuple(keys);
		return value.toArray()[keyFields[0]];
	}
	
	public String toString()
	{
		//return String.valueOf(data.size());
		String[][] dataAsMatrix = new String[data.size()][1 + fieldTypes.length];
		int i = 0;
		for (Tuple tuple : data.values())
		{
			int j = 0;
			dataAsMatrix[i][j++] = String.valueOf(getKey(tuple));
			for (Object value : tuple.toArray())
				dataAsMatrix[i][j++] = value.toString();
			i++;
		}
		return new PrettyPrinter().toString(dataAsMatrix);
	}
	
	public boolean combine(Table otherTable)
	{
		boolean tableChanged = false;
		////System.out.println("Combining isAgg:" + isAggregate + "" + this);
		for (Tuple tuple : otherTable.data.values())
		{
			////System.out.println("with tuple " + tuple);
			boolean tupleChanged = addTuple(tuple);
			if (tupleChanged) tableChanged = true;
		}
		return tableChanged;
	}

	public boolean combineAndSubtract(Table deltaTable)
	{
		setAggregationFunctionType(deltaTable.aggregationFunctionType);
		setRelationalType(deltaTable.relationalType);
		if (deltaTable.isRecursive) setRecursive();
		if (deltaTable.isSourceNodeVariableUnncessary) setSourceNodeVariableUnncessary();
		
		boolean tableChanged = false;
		////System.out.println("Combining isAgg:" + isAggregate + "" + this);
		LinkedList<Tuple> toBeRemoved = new LinkedList<Tuple>();
		for (Tuple tuple : deltaTable.data.values())
		{
			////System.out.println("with tuple " + tuple);
			boolean tupleChanged = addAndSubtractTuple(deltaTable, tuple, toBeRemoved);
			if (tupleChanged) tableChanged = true;
		}
		for (Tuple tuple : toBeRemoved)
			deltaTable.data.remove(getKey(tuple), tuple);
		return tableChanged;
	}

	public void subtract2(Table fullTable)
	{
		////System.out.println("Subtracting:" + isAggregate + "" + otherTable);
		////System.out.println("from " + this);
		for (Tuple tuple : data.values())
		{
			diff(fullTable, tuple);
		}
	}

	public void subtract(Table otherTable)
	{
		////System.out.println("Subtracting:" + isAggregate + "" + otherTable);
		////System.out.println("from " + this);
		for (Tuple tuple : data.values())
		{
			diff(otherTable, tuple);
		}
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
			int key = getKey(value);

			Collection<Tuple> neighbors = neighborsTable.data.get(key);
			for (Tuple n : neighbors)
			{
			int neighborId = (Integer)n.toArray()[1];
			int neighborsSuperVerticesKey = neighborId;
			Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
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
	
	public Map<SuperVertexId,Table> partitionEdgeBased(Table neighborsSuperVerticesTable, HashMap<Integer,SuperVertexId> neighbors)
	{
		Map<SuperVertexId,Table> partitionedTable = new HashMap<SuperVertexId, Table>();
		// The line below should change if source node index changes
		// somehow in the code that rewrites the rule
		// Right now, it is guaranteed to be at 0
		int sourceNodeIdIndex = 0;

		int neighborIdIndex;
		if (isAggregate) neighborIdIndex = fieldTypes.length - 2;
		else neighborIdIndex = fieldTypes.length - 1;

//		System.out.println("Neighbor super vertices table = " + neighborsSuperVerticesTable);
//		System.out.println("Neighbor ID index = " + neighborIdIndex);
		
		for (Tuple value : data.values())
		{
			int key = getKey(value);
			int[] tupleArray = value.toArray();
			int neighborId = tupleArray[neighborIdIndex];
			SuperVertexId neighborSuperVertexId= null;
			if((neighborSuperVertexId = neighbors.get(neighborId)) == null)
			{
				 
//				System.out.println("Neighbor id = " + neighborId);
				int neighborsSuperVerticesKey = neighborId;
				Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(
						neighborsSuperVerticesKey).iterator().next();
//				System.out.println("Neighbor super vertex tuple = " + neighborSuperVertexTuple);
				int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
				neighborSuperVertexId = new SuperVertexId(
						Integer.valueOf(neighborSuperVertexArray[1]).shortValue(), 
						neighborSuperVertexArray[2]);
				neighbors.put(neighborId, neighborSuperVertexId);
			}
//			else
//			{
//				System.out.println("Found neighbor:"+ neighborId +" , super=" + neighborSuperVertexId);
//			}
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

	
	public Map<SuperVertexId,PartitionWithMessages> partitionWithMessages(Table neighborsTable, Table neighborsSuperVerticesTable, Table messagesTable, Table otherDirectionNeighborsTable, boolean isPagerank)
	{
		Map<SuperVertexId,PartitionWithMessages> partitionedTableWithMessages = new HashMap<SuperVertexId, PartitionWithMessages>();		// The line below should change if source node index changes

		for (Tuple value : data.values())
		{
			int key = getKey(value);
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
				int neighborsSuperVerticesKey = neighborId;
				Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
				int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
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
				if (!existingPartitionWithMessages.partition.data.contains(key, value))
					existingPartitionWithMessages.partition.data.put(key, value);
				addSingleMessage(neighborId, iterationNumber, existingPartitionWithMessages.messages);
			}
			//resetNumberOfMessages(key, iterationNumber, messagesTable);
		}
		return partitionedTableWithMessages;
	}
	
	public Map<SuperVertexId,PartitionWithMessages> partitionWithMessagesEdgeBased(Table neighborsSuperVerticesTable, Table messagesTable, Table otherDirectionNeighborsTable, boolean isPagerank)
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
			int[] tupleArray = value.toArray();
			int destNodeIdTuple = getKey(value);

			int iterationNumber = 1;

			//For PageRank
			iterationNumber = (Integer)(value.toArray()[1]);

			int sourceNodeIdTuple = tupleArray[sourceNodeIdIndex];
			Tuple sourceNodeIdAndIterationNumberTuple = new Tuple(new int[]{tupleArray[sourceNodeIdIndex], iterationNumber});

			int numberOfRecievedMessages = getNumberOfMessages(sourceNodeIdTuple, iterationNumber - 1, messagesTable);
			int numberOfOtherSideNeighbors = getNumberOfNeighbors(sourceNodeIdTuple, otherDirectionNeighborsTable);
			
			if (isPagerank)
				{ if (isRecursive && iterationNumber > 0 && numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}
			
			else
				{ if (numberOfRecievedMessages != numberOfOtherSideNeighbors) continue; }

			int neighborId = (Integer)tupleArray[neighborIdIndex];
			int neighborsSuperVerticesKey = neighborId;
			Tuple neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
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
		//for (Tuple processedSourceNodeIdAndIterationNumberTuple : processedSourceNodeIdAndIterationNumberTuples)
		//{
			//Tuple sourceNodeIdTuple = new Tuple(new Object[]{processedSourceNodeIdAndIterationNumberTuple.toArray()[0]});
			//int iterationNumber = (int)processedSourceNodeIdAndIterationNumberTuple.toArray()[1];
			//resetNumberOfMessages(sourceNodeIdTuple, iterationNumber, messagesTable);
		//}
		return partitionedTableWithMessages;
	}

	Table createMessageTable()
	{
		int[] messagesKeyFields = new int[]{0};
		Class[] messagesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
		Table messagesTable = new Table("messages_full",messagesFieldTypes, messagesKeyFields, metadata);
		messagesTable.setAggregationFunctionType(AggregationFunctionType.SUM);
		messagesTable.setAggregate();	
		return messagesTable;
	}
	
	void addSingleMessage(int destinationVertexId, int iterationNumber, Table messagesTable)
	{
		int[] messagesTuple = new int[3];
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

	int getNumberOfMessages(int key, int iterationNumber, Table messagesTable)
	{
		for (Tuple messageTuple : messagesTable.data.get(key))
		{
			int[] messageTupleArray = messageTuple.toArray();
			if ((Integer)messageTupleArray[1] == iterationNumber) return (Integer)messageTupleArray[2];
		}
		return 0;
	}
	
	void resetNumberOfMessages(int key, int iterationNumber, Table messagesTable)
	{
		Tuple toDelete = null;
		for (Tuple messageTuple : messagesTable.data.get(key))
		{
			int[] messageTupleArray = messageTuple.toArray();
			if ((Integer)messageTupleArray[1] == iterationNumber) toDelete = messageTuple;
		}
		if (toDelete != null) messagesTable.data.remove(key, toDelete);
	}

	
int getNumberOfNeighbors(int key, Table neighborsTable)
	{
		int numberOfNeighbors = 0;
		for (Tuple t : neighborsTable.data.get(key))
			//if ((Integer)t.toArray()[1] <= 30) 
				numberOfNeighbors++;
		return numberOfNeighbors;
		//return neighborsTable.data.get(key).size();
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
		//data = HashMultimap.create();
		for (int k = 0; k < size; k++)
		{
			int[] array = new int[fieldTypes.length];
			for (int i = 0; i < fieldTypes.length; i++)
			{
				if (fieldTypes[i] == Integer.class) array[i] = WritableUtils.readVInt(in);
				else throw new RuntimeException("Unsupported data type");
//				else if (fieldTypes[i] == String.class) array[i] = WritableUtils.readCompressedString(in);
//				else if (fieldTypes[i] == Boolean.class) array[i] = (WritableUtils.readVInt(in) == 1);
			}
			Tuple tuple = new Tuple(array);
			addTuple(tuple);
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//byte[] byteArray = WritableUtils.readCompressedByteArray(in);
		//int arrayLength = in.readInt();
		//byte[] byteArray = new byte[arrayLength];
		//in.readFully(byteArray);
		//DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(byteArray));
		
//		System.out.println("Metadata in reading table " + this.name + " = " + metadata);
		int nFieldTypes = in.readInt();
//		isRecursive = in.readBoolean();
		isSourceNodeVariableUnncessary = in.readBoolean();
//		relationalType = RelationalType.values()[in.readByte()];
//		setAggregationFunctionType(AggregationFunctionType.values()[in.readByte()]);
		fieldTypes = new Class[nFieldTypes];
		
//		-------------------------------------------
//		for (int i = 0; i < nFieldTypes; i++)
//		{
//			byte fieldType = in.readByte();
//			if (fieldType == 1) fieldTypes[i] = Integer.class;
//			else if (fieldType == 0) fieldTypes[i] = String.class;
//			else if (fieldType == 2) fieldTypes[i] = Boolean.class;
//		}
//		int nKeyFields = in.readInt();
//		---------------------------------------------
		
		keyFields = new int[1];
		keyFields[0] = in.readInt();

		size = in.readInt();
		data = new Multimap<>();
		int index = isSourceNodeVariableUnncessary? 1:0;
			for (int k = 0; k < size; k++)
			{
				
				int[] array = new int[nFieldTypes];
				if (isSourceNodeVariableUnncessary)
				{
					array[0] = 0;
				}
				for (int i = index; i < nFieldTypes; i++)
				{
					array[i] = in.readInt();
				}
				Tuple tuple = new Tuple(array);
				addTuple(tuple);
			}
		
		//inStream.close();
	}


	@Override
	public void write(DataOutput out) throws IOException {
		//ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
		//DataOutputStream outStream = new DataOutputStream(byteArrayOutStream);
		//UnsafeByteArrayOutputStream outStream = new UnsafeByteArrayOutputStream();
		
		out.writeInt(fieldTypes.length);
//		out.writeBoolean(isRecursive);
		out.writeBoolean(isSourceNodeVariableUnncessary);		
//		out.writeByte(relationalType.ordinal());
//		out.writeByte(aggregationFunctionType.ordinal());
		
//		-----------------------------------------------
//		for (Class fieldType : fieldTypes)
//			if (fieldType == Integer.class) out.writeByte(1);
//			else if (fieldType == String.class) out.writeByte(0);
//			else if (fieldType == Boolean.class) out.writeByte(2);
//
//		out.writeInt( keyFields.length);
//		for (int keyField : keyFields)
//		--------------------------------------------
		
		//TODO assumption keys of a table is only one
		out.writeInt(keyFields[0]);
		
		out.writeInt(data.size());
		int index;
		index = isSourceNodeVariableUnncessary? 1:0;
//		System.out.println("In write table " + this.name);
//		System.out.println("Table data = " + this);
//		System.out.println("Printing stack trace:");
//		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
//		for (int i = 1; i < elements.length; i++) {
//			StackTraceElement s = elements[i];
//			System.out.println("\tat " + s.getClassName() + "." + s.getMethodName()
//					+ "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
//		}
		for (Tuple tuple : data.values())
		{
			int[] array = tuple.toArray(); 
			for (int i = index; i < array.length; i++)
			{
				out.writeInt(array[i]);
//				System.out.println("Sending: " + array[i]);
			}
		}
		
		
		
		//outStream.flush();
		//WritableUtils.writeCompressedByteArray(out, byteArrayOutStream.toByteArray());
		//byte[] byteArray = outStream.toByteArray();
		//out.writeInt(byteArray.length);
		//out.write(byteArray);
		//outStream.close();
	}
}