package schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import algebra.RelationalType;
import giraph.SuperVertexId;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import maputil.Multimap;
import utils.AggregationFunctionType;
import utils.PrettyPrinter;



public class Table implements Writable {

	public enum edb_columns { 
		VERTICES(2), EDGES(3), NEIGHBORS(3), MESSAGES(3);
		
		private int value;
		private edb_columns(int value) { this.value = value; }
		
	}
	
	private String name;
	private Class[] fieldTypes;
	private int[] keyFields;
	private Multimap data;
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
	
	public Table(Class[] fieldTypes, int[] keyFields, String name)
	{
		this.fieldTypes = fieldTypes;
		this.keyFields = keyFields;
		this.data = new Multimap();
		this.name = name;
	}
	
	
	public Table(Class[] fieldTypes, int[] keyFields, int initialSize)
	{
		this.fieldTypes = fieldTypes;
		this.keyFields = keyFields;
		this.data = new Multimap(initialSize);
	}
	
	public void finalize() {
		data.finalize();
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
	
	public void setName(String name){
		this.name = name;
	}
	
	public RelationalType getRelationalType()
	{
		return relationalType;
	}

	public boolean addTuple(int[] tuple)
	{
		int key = getKey(tuple);
		return addTuple(key, tuple);
	}
	
	public void putTuple(int[] tuple)
	{
		int key = getKey(tuple);
		data.put(key, tuple);
	}
	
	public boolean addTuple(int key, int[] value)
	{
		if (!isAggregate && data.contains(key, value)) return false; 
		if (isAggregate)
		{
			//int[] toBeInserted = value;
			int aggregateArgIndex = value.length - 1;
			for (int[] t : data.get(key))
			{
				//int[] existing = t;
				boolean equalGroup = true;
				if (aggregateArgIndex > 1)
					for (int i = 0; i < aggregateArgIndex; i++)
						if (value[i] != (t[i]))
						{
							equalGroup = false;
							break;
						}
				if (equalGroup)
				{					
					int existingValueBeforeCombining = (Integer)t[aggregateArgIndex];
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.SUM){
						t[aggregateArgIndex] = value[aggregateArgIndex] + 
						t[aggregateArgIndex];
					}
					else if (aggregationFunctionType == AggregationFunctionType.MIN) {
						if (value[aggregateArgIndex] < t[aggregateArgIndex]) {
							t[aggregateArgIndex]= value[aggregateArgIndex];
						}
					}
					else if (aggregationFunctionType == AggregationFunctionType.FSUM){
						t[aggregateArgIndex] = t[aggregateArgIndex] = Float.floatToIntBits(Float.intBitsToFloat(value[aggregateArgIndex]) +  
						Float.intBitsToFloat(t[aggregateArgIndex])); 

					}
					
					if (t[aggregateArgIndex] == existingValueBeforeCombining) 
						return false; 
					else return true;
					
				}				
			}
		}
//		System.out.println("Add tuple key = " + key + ", value = " + value);
		data.put(key, value);
		return true;
	}
	
	public boolean addAndSubtractTuple(Table deltaTable, int[] deltaTuple, LinkedList<int[]> toBeRemoved)
	{
		int deltaKey = getKey(deltaTuple);
		int[] deltaValue = deltaTuple;
//		System.out.println("base table is aggregate:" + isAggregate);
//		System.out.println("delta table is aggregate:" + deltaTable.isAggregate);
		if (!isAggregate && data.contains(deltaKey, deltaValue)) 
		{ 
			toBeRemoved.add(deltaValue); 
			return false; 
		} 
		if (isAggregate || deltaTable.isAggregate)
		{
			//int[] deltaValue = deltaValue.toArray();
			int aggregateArgIndex = deltaValue.length - 1;
			for (int[] baseValue : data.get(deltaKey))
			{
				//int[] baseValue = baseValue.toArray();
				boolean equalGroup = true;
				if (aggregateArgIndex > 1)
					for (int i = 0; i < aggregateArgIndex; i++)
						if (deltaValue[i] != (baseValue[i]))
						{
							equalGroup = false;
							break;
						}
				if (equalGroup)
				{				
					//FOR UPDATING DELTA
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.MIN)
						if (((Integer)deltaValue[aggregateArgIndex]).intValue() >= 
						((Integer) baseValue[aggregateArgIndex]).intValue())
						{
							toBeRemoved.add(deltaValue); 
							return false;
						}

					int existingValueBeforeCombining = (Integer)baseValue[aggregateArgIndex];
					//FOR UPDATING BASE
					//IN CASE OF INCREMENTAL MAINTAINANCE:
					if (aggregationFunctionType == AggregationFunctionType.SUM)
						baseValue[aggregateArgIndex] = (Integer)deltaValue[aggregateArgIndex] +
						(Integer)baseValue[aggregateArgIndex];
					else if (aggregationFunctionType == AggregationFunctionType.MIN) {
						if (((Integer)deltaValue[aggregateArgIndex]).intValue() < 
								((Integer)baseValue[aggregateArgIndex]).intValue()) 
						{
							baseValue[aggregateArgIndex] = deltaValue[aggregateArgIndex]; 
						}
					}
					else if (aggregationFunctionType == AggregationFunctionType.FSUM)
						baseValue[aggregateArgIndex] = Float.floatToIntBits(Float.intBitsToFloat((Integer)deltaValue[aggregateArgIndex]) + 
						Float.intBitsToFloat((Integer)baseValue[aggregateArgIndex])); 
						
					if ((Integer)baseValue[aggregateArgIndex] == existingValueBeforeCombining) 
						return false; 
					else return true;
				}				
			}			
		}
		data.put(deltaKey, deltaValue);
		return true;	
	}

	public void diff(Table otherTable, int[] tuple)
	{
		int key = getKey(tuple);
		int[] value = tuple;
		if (!isAggregate && otherTable.data.contains(key, value)) { data.remove(key, value); return;} 
		if (isAggregate)
		{
			int[] toBeInserted = value;
			int aggregateArgIndex = toBeInserted.length - 1;
			int[] existingAggregateTuple = null;
			for (int[] t : otherTable.data.get(key))
			{
				int[] existing = t;
				boolean equalGroup = true;
				if (aggregateArgIndex > 1)	
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
					if (aggregationFunctionType == AggregationFunctionType.SUM) {
						toBeInserted[aggregateArgIndex] =  (Integer)toBeInserted[aggregateArgIndex] - 
								(Integer)existing[aggregateArgIndex];
					}
					else if (aggregationFunctionType == AggregationFunctionType.FSUM) {
						toBeInserted[aggregateArgIndex] =  Float.floatToIntBits(Float.intBitsToFloat((Integer)toBeInserted[aggregateArgIndex]) -  
						Float.intBitsToFloat((Integer)existing[aggregateArgIndex])); 
					}
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
	
	public Multimap getData()
	{
		return data;
	}
	
	public int getKey(int[] value)
	{
		assert(keyFields.length == 1);
//		System.out.println("Add tupe " + value);
//		System.out.println("Keyfields = " + Arrays.toString(keyFields));
		//int[] keys = new int[keyFields.length];
		//int[] tupleArray = value.toArray();
		
//		int i = 0;
//		for (int keyField : keyFields) keys[i++] = tupleArray[keyField];
//		int[] key = new int[](keys);
		return value[keyFields[0]];
	}
	
	public String toString()
	{
//		System.out.println("Field types = " + fieldTypes.length);
		//return String.valueOf(data.size());
		String[][] dataAsMatrix = new String[data.size()][1 + fieldTypes.length];
		int i = 0;
		for (int[] tuple : data.values())
		{
			int j = 0;
//			System.out.println("Key = " + String.valueOf(getKey(tuple)));
//			System.out.println("Value = " + Arrays.toString(tuple.toArray()));
			dataAsMatrix[i][j++] = String.valueOf(getKey(tuple));
			for (Object value : tuple)
				dataAsMatrix[i][j++] = value.toString();
			i++;
		}
		return new PrettyPrinter().toString(dataAsMatrix);
	}
	
	public boolean combine(Table otherTable)
	{
		boolean tableChanged = false;
		////System.out.println("Combining isAgg:" + isAggregate + "" + this);
		for (int[] tuple : otherTable.data.values())
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
		LinkedList<int[]> toBeRemoved = new LinkedList<int[]>();
		for (int[] tuple : deltaTable.data.values())
		{
			////System.out.println("with tuple " + tuple);
			boolean tupleChanged = addAndSubtractTuple(deltaTable, tuple, toBeRemoved);
			if (tupleChanged) tableChanged = true;
		}
		for (int[] tuple : toBeRemoved)
			deltaTable.data.remove(getKey(tuple), tuple);
		return tableChanged;
	}

	public void subtract2(Table fullTable)
	{
		////System.out.println("Subtracting:" + isAggregate + "" + otherTable);
		////System.out.println("from " + this);
		for (int[] tuple : data.values())
		{
			diff(fullTable, tuple);
		}
	}

	public void subtract(Table otherTable)
	{
		////System.out.println("Subtracting:" + isAggregate + "" + otherTable);
		////System.out.println("from " + this);
		for (int[] tuple : data.values())
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
		for (int[] value : data.values())
		{
			int key = getKey(value);

			Collection<int[]> neighbors = neighborsTable.data.get(key);
			for (int[] n : neighbors)
			{
			int neighborId = (Integer)n[1];
			int neighborsSuperVerticesKey = neighborId;
			int[] neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			
			SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexTuple[1])).shortValue(), (Integer)(neighborSuperVertexTuple[2]));
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
	
	public Map<SuperVertexId,Table> partitionEdgeBased(
			Table neighborsSuperVerticesTable,
			Int2ObjectOpenHashMap<SuperVertexId> neighbors)
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
		
		for (int[] value : data.values())
		{
			int key = getKey(value);
			//int[] tupleArray = value.toArray();
			int neighborId = value[neighborIdIndex];
			SuperVertexId neighborSuperVertexId= null;
			if((neighborSuperVertexId = neighbors.get(neighborId)) == null)
			{
				 
//				System.out.println("Neighbor id = " + neighborId);
				int neighborsSuperVerticesKey = neighborId;
				int[] neighborSuperVertexTuple = null;
				neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(
						neighborsSuperVerticesKey).iterator().next();
//				System.out.println("Neighbor super vertex tuple = " + neighborSuperVertexTuple);
				//int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
				neighborSuperVertexId = new SuperVertexId(
						Integer.valueOf(neighborSuperVertexTuple[1]).shortValue(), 
						neighborSuperVertexTuple[2]);
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
				//if (isSourceNodeVariableUnncessary) tupleArray[sourceNodeIdIndex] = 0;
				//existingTable.data.put(key, value);
				existingTable.addTuple(key, value);
			}
		}
		return partitionedTable;
	}

	
	public Map<SuperVertexId,PartitionWithMessages> partitionWithMessages(Table neighborsTable, Table neighborsSuperVerticesTable, Table messagesTable, Table otherDirectionNeighborsTable, boolean isPagerank)
	{
		Map<SuperVertexId,PartitionWithMessages> partitionedTableWithMessages = new HashMap<SuperVertexId, PartitionWithMessages>();		// The line below should change if source node index changes

		for (int[] value : data.values())
		{
			int key = getKey(value);
			int iterationNumber = 1;
			
			//For PageRank
			iterationNumber = (Integer)(value[1]);
			
			int numberOfRecievedMessages = getNumberOfMessages(key, iterationNumber - 1, messagesTable);
			int numberOfOtherSideNeighbors = getNumberOfNeighbors(key, otherDirectionNeighborsTable);
			if (isPagerank)
				{ if (isRecursive && iterationNumber > 0 && numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}
			else
				{ if (numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}

			Collection<int[]> neighbors = neighborsTable.data.get(key);
			for (int[] n : neighbors)
			{
				int neighborId = (Integer)n[1];
				int neighborsSuperVerticesKey = neighborId;
				int[] neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
				//int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
				SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexTuple[1])).shortValue(), (Integer)(neighborSuperVertexTuple[2]));
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
		
		Set<int[]> processedSourceNodeIdAndIterationNumberTuples = new HashSet<>();
		for (int[] value : data.values())
		{
			//int[] tupleArray = value.toArray();
			int destNodeIdTuple = getKey(value);

			int iterationNumber = 1;

			//For PageRank
			iterationNumber = (Integer)(value[1]);

			int sourceNodeIdTuple = value[sourceNodeIdIndex];
			int[] sourceNodeIdAndIterationNumberTuple = new int[]{value[sourceNodeIdIndex], iterationNumber};

			int numberOfRecievedMessages = getNumberOfMessages(sourceNodeIdTuple, iterationNumber - 1, messagesTable);
			int numberOfOtherSideNeighbors = getNumberOfNeighbors(sourceNodeIdTuple, otherDirectionNeighborsTable);
			
			if (isPagerank)
				{ if (isRecursive && iterationNumber > 0 && numberOfRecievedMessages != numberOfOtherSideNeighbors) continue;}
			
			else
				{ if (numberOfRecievedMessages != numberOfOtherSideNeighbors) continue; }

			int neighborId = value[neighborIdIndex];
			int neighborsSuperVerticesKey = neighborId;
			int[] neighborSuperVertexTuple = neighborsSuperVerticesTable.data.get(neighborsSuperVerticesKey).iterator().next();
			//int[] neighborSuperVertexArray = neighborSuperVertexTuple.toArray();
			SuperVertexId neighborSuperVertexId = new SuperVertexId(((Integer)(neighborSuperVertexTuple[1])).shortValue(), (Integer)(neighborSuperVertexTuple[2]));
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
				if (isSourceNodeVariableUnncessary) value[sourceNodeIdIndex] = 0;
				existingPartitionWithMessages.partition.addTuple(destNodeIdTuple, value);
			}
			addSingleMessage(neighborId, iterationNumber, existingPartitionWithMessages.messages);
			
			processedSourceNodeIdAndIterationNumberTuples.add(sourceNodeIdAndIterationNumberTuple);
		}
		//for (int[] processedSourceNodeIdAndIterationNumberTuple : processedSourceNodeIdAndIterationNumberTuples)
		//{
			//int[] sourceNodeIdTuple = new int[](new Object[]{processedSourceNodeIdAndIterationNumberTuple.toArray()[0]});
			//int iterationNumber = (int)processedSourceNodeIdAndIterationNumberTuple.toArray()[1];
			//resetNumberOfMessages(sourceNodeIdTuple, iterationNumber, messagesTable);
		//}
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
		int[] messagesTuple = new int[3];
		messagesTuple[0] = destinationVertexId;
		messagesTuple[1] = iterationNumber;
		messagesTuple[2] = 1;
		messagesTable.addTuple(messagesTuple);		
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
		for (int[] messageTuple : messagesTable.data.get(key))
		{
			int[] messageTupleArray = messageTuple;
			if ((Integer)messageTupleArray[1] == iterationNumber) return (Integer)messageTupleArray[2];
		}
		return 0;
	}
	
	void resetNumberOfMessages(int key, int iterationNumber, Table messagesTable)
	{
		int[] toDelete = null;
		for (int[] messageTuple : messagesTable.data.get(key))
		{
			int[] messageTupleArray = messageTuple;
			if ((Integer)messageTupleArray[1] == iterationNumber) toDelete = messageTuple;
		}
		if (toDelete != null) messagesTable.data.remove(key, toDelete);
	}

	
int getNumberOfNeighbors(int key, Table neighborsTable)
	{
		int numberOfNeighbors = 0;
		for (int[] t : neighborsTable.data.get(key))
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
			addTuple(array);
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
			data = new Multimap();
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
				putTuple(array);
			}	
		
	}


	@Override
	public void write(DataOutput out) throws IOException {

//			System.out.println("Write field types length = " + fieldTypes.length);
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
			
			int index;
			index = isSourceNodeVariableUnncessary? 1:0;
	
			for (int[] tuple : data.values())
			{
				for (int i : tuple)
				{
					out.writeInt(i);
				}
			}			
			
			
	}
}
