package schema;

import giraph.DatalogWorkerContext;
import giraph.SuperVertexId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.Writable;

import parser.Expression;
import schema.Table.PartitionWithMessages;
import algebra.RelationalType;

public class Database implements Writable {
	
	private Map<String, Table> tables = new HashMap<String, Table>();
	private int size = 0;
	private Metadata metadata;
	private int superstep = 0;
	
	public Database()
	{
		tables = new HashMap<String, Table>();
		size = 0;
	}
	
	public Database(Metadata m, int superstep)
	{
		tables = new HashMap<String, Table>();
		size = 0;
		this.metadata = m;
		this.superstep = superstep;
	}
	
	
	public Table getDataTableByName(String name)
	{
		return tables.get(name);
	}
	
	public void addDataTable(String name, Table dataTable)
	{
		tables.put(name, dataTable);
	}
	
	public void removeDataTable(String name)
	{
		tables.remove(name);
	}
	
	public void removeRelationalDeltaTables()
	{
		for (Iterator<Entry<String,Table>> tablesIterator = tables.entrySet().iterator(); tablesIterator.hasNext();)
		{
			Entry<String,Table> t = tablesIterator.next();
			String tableName = t.getKey();
			Table table = t.getValue();
			if (!(table.getRelationalType() == RelationalType.NOT_RELATIONAL) && !tableName.endsWith("_full"))
				tablesIterator.remove();
		}
	}
	
	public boolean exists(String name)
	{
		return tables.containsKey(name);
	}
	
	public Class[] getTypes(List<Expression> outputFields)
	{
		int numberOfOutputFields = outputFields.size();
		Class[] types = new Class[numberOfOutputFields];
		for (int i=0; i<numberOfOutputFields; i++)
			types[i] = outputFields.get(i).getType(this);
		return types;
	}

	public void readFields(DataInput in) throws IOException {
		//byte[] uncompressedByteArray = WritableUtils.readCompressedByteArray(in);
		//UnsafeByteArrayInputStream inStream = new UnsafeByteArrayInputStream(uncompressedByteArray);
//		System.out.println("Metadata in read database = " + metadata);
		size = in.readInt();
		tables = new HashMap<String, Table>();
		for (int i = 0; i < size; i++)
		{
			//TODO Vicky test for performance
			String tableName = in.readUTF();
//			String tableName = String.valueOf(in.readInt());
//			System.out.println("Table name = " + tableName);
			Table table = new Table(tableName,null, null, metadata);
			table.readFields(in);
			tables.put(tableName, table);
		}
	}

	public void readFieldsC(DataInput in) throws IOException {
		int decompressedLength = in.readInt();
		int compressedLength = in.readInt();
		byte[] compressed = new byte[compressedLength];
		in.readFully(compressed, 0, compressedLength);
		LZ4Factory factory = LZ4Factory.fastestInstance();
		LZ4FastDecompressor decompressor = factory.fastDecompressor();
		byte[] uncompressedByteArray = new byte[decompressedLength];
		decompressor.decompress(compressed, 0, uncompressedByteArray, 0, decompressedLength);		
		//byte[] uncompressedByteArray = WritableUtils.readCompressedByteArray(in);
		UnsafeByteArrayInputStream inStream = new UnsafeByteArrayInputStream(uncompressedByteArray);

		size = inStream.readInt();
		tables = new HashMap<String, Table>();
		for (int i = 0; i < size; i++)
		{
			String tableName = inStream.readUTF();
			Table table = new Table(null, null);
			table.readFields(inStream);
			tables.put(tableName, table);
		}
	}

	public void write(DataOutput out) throws IOException {
//		System.out.println("Metadata in write database = " + metadata);
		out.writeInt(tables.entrySet().size());
		for (Map.Entry<String, Table> entry : tables.entrySet())
		{
			String tableName = entry.getKey();
//			System.out.println("Write table: " + tableName);
			Table table = entry.getValue();
			//Vicky TODO: testing for performance optimization
			out.writeUTF(tableName);
//			out.writeInt(metadata.name_to_id.get(tableName));
			table.write(out);
		}
	}
	
	public void writeC(DataOutput out) throws IOException {
		UnsafeByteArrayOutputStream outStream = new UnsafeByteArrayOutputStream();
		outStream.writeInt(tables.entrySet().size());
		for (Map.Entry<String, Table> entry : tables.entrySet())
		{
			String tableName = entry.getKey();
			Table table = entry.getValue();
			outStream.writeUTF(tableName);
			table.write(outStream);
		}
		outStream.flush();
		//WritableUtils.writeCompressedByteArray(out, outStream.toByteArray());
		LZ4Factory factory = LZ4Factory.fastestInstance();
		LZ4Compressor compressor = factory.fastCompressor();
		byte[] outStreamByteArray = outStream.toByteArray();
		int decompressedLength = outStreamByteArray.length;
		int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
		byte[] compressed = new byte[maxCompressedLength];
		int compressedLength = compressor.compress(outStreamByteArray, 0, decompressedLength, compressed, 0, maxCompressedLength);
		out.writeInt(decompressedLength);
		out.writeInt(compressedLength);
		out.write(compressed, 0, compressedLength);
		outStream.close();
	}

	public Set<String> combine(Database otherDatabase)
	{
		Set<String> changedTables = new HashSet<String>();
		for (Entry<String, Table> entry : otherDatabase.tables.entrySet())
		{
			String tableName = entry.getKey();
			if (tableName.endsWith("_full")) continue;
			Table otherTable = entry.getValue();
			Table thisTable = tables.get(tableName + "_full");
			if (thisTable != null) 
			{
				boolean tableChanged = thisTable.combine(otherTable);
				if (tableChanged) changedTables.add(tableName);
			}
			else
			{
				tables.put(tableName + "_full", otherTable);
				if (!otherTable.isEmpty()) changedTables.add(tableName);
			}
			tables.put(tableName, otherTable);
		}
		return changedTables;
	}
	
	public Set<String> combine2(Database otherDatabase)
	{
		Set<String> changedTables = new HashSet<String>();
		for (Entry<String, Table> entry : otherDatabase.tables.entrySet())
		{
			String tableName = entry.getKey();
			Table otherTable = entry.getValue();
			Table thisTable = tables.get(tableName);
			if (thisTable != null) 
			{
				boolean tableChanged = thisTable.combine(otherTable);
				if (tableChanged) changedTables.add(tableName);
			}
			else
			{
				tables.put(tableName, otherTable);
				if (!otherTable.isEmpty()) changedTables.add(tableName);
			}
		}
		return changedTables;
	}

	public void substract(Database otherDatabase)
	{
		for (Entry<String, Table> entry : tables.entrySet())
		{
			String tableName = entry.getKey();
			Table thisTable = entry.getValue();
			Table otherTable = otherDatabase.tables.get(tableName + "_full");
			if (otherTable != null) 
			{
				thisTable.subtract(otherTable);
			}
			//else tables.put(tableName, thisTable);
		}
	}

	public void substract1(Database delta)
	{
		for (Entry<String, Table> entry : delta.tables.entrySet())
		{
			String tableName = entry.getKey();
			Table thisTable = entry.getValue();
			Table fullTable = tables.get(tableName + "_full");
			if (fullTable != null) 
			{
				thisTable.subtract(fullTable);
			}
			//else tables.put(tableName, thisTable);
		}
	}

	public Set<String> refresh(Database delta)
	{
		Set<String> changedTables = new HashSet<String>();
		for (Entry<String, Table> entry : delta.tables.entrySet())
		{
			String deltaTableName = entry.getKey();
			if (deltaTableName.endsWith("_full")) continue;
			Table deltaTable = entry.getValue();
			Table fullTable = tables.get(deltaTableName + "_full");
			if (fullTable != null) 
			{
//				System.out.println("Combine full table " + fullTable.toString() + " with delta table " + deltaTable);
				boolean tableChanged = fullTable.combineAndSubtract(deltaTable);
				if (tableChanged) changedTables.add(deltaTableName);
			}
			else
			{
//				System.out.println("Add full table " + deltaTableName + "_full");
				tables.put(deltaTableName + "_full", deltaTable);
				if (!deltaTable.isEmpty()) changedTables.add(deltaTableName);
			}
//			System.out.println("Add table " + deltaTableName);
			tables.put(deltaTableName, deltaTable);
		}
		return changedTables;
	}

	public Database getRelationalDatabase()
	{
		Database relationalDatabase = new Database();
		for (Entry<String, Table> entry : tables.entrySet())
		{
			String tableName = entry.getKey();
			Table table = entry.getValue();
			if (!(table.getRelationalType() == RelationalType.NOT_RELATIONAL)) 
				relationalDatabase.addDataTable(tableName, table);
		}
		return relationalDatabase;
	}
	
	public Map<SuperVertexId,Database> getDatabasesForEverySuperVertex(Database inputDatabase)
	{
		Map<SuperVertexId,Database> partitionedDatabase = new HashMap<>(); 
		Database relationalDatabase = getRelationalDatabase();
		for (Entry<String,Table> entry : relationalDatabase.tables.entrySet())
		{
			String tableName = entry.getKey();
			Table table = entry.getValue();
			
			Map<SuperVertexId,Table> outgoingPartitionedTable = new HashMap<>();
			Map<SuperVertexId,Table> incomingPartitionedTable = new HashMap<>();
			if (table.getRelationalType() == RelationalType.OUTGOING_RELATIONAL || table.getRelationalType() == RelationalType.TWO_WAY_RELATIONAL)
				outgoingPartitionedTable = table.partition(inputDatabase.tables.get("outgoingNeighbors"), inputDatabase.tables.get("neighborSuperVertices"));
			
			if (table.getRelationalType() == RelationalType.INCOMING_RELATIONAL || table.getRelationalType() == RelationalType.TWO_WAY_RELATIONAL)
				incomingPartitionedTable = table.partition(inputDatabase.tables.get("incomingNeighbors"), inputDatabase.tables.get("neighborSuperVertices"));

			Map<SuperVertexId,Table> partitionedTable = new HashMap<>();
			partitionedTable.putAll(outgoingPartitionedTable);
			partitionedTable.putAll(incomingPartitionedTable);
			
			for (Entry<SuperVertexId,Table> partitionEntry : partitionedTable.entrySet())
			{
				SuperVertexId superVertexId = partitionEntry.getKey();
				Table tablePartition = partitionEntry.getValue();
				Database existingDatabase = partitionedDatabase.get(superVertexId);
				if (existingDatabase == null) 
				{
					existingDatabase = new Database();
					partitionedDatabase.put(superVertexId, existingDatabase);
				}
				existingDatabase.addDataTable(tableName, tablePartition);
			}
		}
		return partitionedDatabase;
	}

	public Map<SuperVertexId,Database> getDatabasesForEverySuperVertexEdgeBased(Database inputDatabase, HashMap<Integer, SuperVertexId> neighbors, Metadata metadata)
	{
		Map<SuperVertexId,Database> partitionedDatabase = new HashMap<>(); 
		Database relationalDatabase = getRelationalDatabase();
		for (Entry<String,Table> entry : relationalDatabase.tables.entrySet())
		{
			String tableName = entry.getKey();
			Table table = entry.getValue();
			
//			if(metadata.name_to_id.get(tableName) == null)
//				metadata.name_to_id.put(tableName, metadata.name_to_id.size()+1);					

			Map<SuperVertexId,Table> partitionedTable = new HashMap<>();
			partitionedTable = table.partitionEdgeBased(inputDatabase.tables.get("neighborSuperVertices"), neighbors);
			
			for (Entry<SuperVertexId,Table> partitionEntry : partitionedTable.entrySet())
			{
				SuperVertexId superVertexId = partitionEntry.getKey();
				Table tablePartition = partitionEntry.getValue();
				Database existingDatabase = partitionedDatabase.get(superVertexId);
				if (existingDatabase == null) 
				{
					existingDatabase = new Database();
					partitionedDatabase.put(superVertexId, existingDatabase);
				}
				existingDatabase.addDataTable(tableName, tablePartition);
			}
		}
		return partitionedDatabase;
	}

	public Map<SuperVertexId,Database> getDatabasesForEverySuperVertexWithMessagesEdgeBased(Database inputDatabase, boolean isPagerank)
	{
		Map<SuperVertexId,Database> partitionedDatabase = new HashMap<>(); 
		Database relationalDatabase = getRelationalDatabase();
		for (Entry<String,Table> entry : relationalDatabase.tables.entrySet())
		{
			String tableName = entry.getKey();
			Table table = entry.getValue();
			
			Map<SuperVertexId,PartitionWithMessages> outgoingPartitionedTable = new HashMap<>();
			Map<SuperVertexId,PartitionWithMessages> incomingPartitionedTable = new HashMap<>();
			if (table.getRelationalType() == RelationalType.OUTGOING_RELATIONAL || table.getRelationalType() == RelationalType.TWO_WAY_RELATIONAL)
				outgoingPartitionedTable = table.partitionWithMessagesEdgeBased(inputDatabase.tables.get("neighborSuperVertices"), inputDatabase.tables.get("messages_full"), inputDatabase.tables.get("incomingNeighbors"), isPagerank);
			
			if (table.getRelationalType() == RelationalType.INCOMING_RELATIONAL || table.getRelationalType() == RelationalType.TWO_WAY_RELATIONAL)
				incomingPartitionedTable = table.partitionWithMessagesEdgeBased(inputDatabase.tables.get("neighborSuperVertices"), inputDatabase.tables.get("messages_full"), inputDatabase.tables.get("outgoingNeighbors"), isPagerank);

			Map<SuperVertexId,PartitionWithMessages> partitionedTable = new HashMap<>();
			partitionedTable.putAll(outgoingPartitionedTable);
			partitionedTable.putAll(incomingPartitionedTable);
			
			for (Entry<SuperVertexId,PartitionWithMessages> partitionEntry : partitionedTable.entrySet())
			{
				SuperVertexId superVertexId = partitionEntry.getKey();
				Table tablePartition = partitionEntry.getValue().partition;
				Table messages = partitionEntry.getValue().messages;
				Database existingDatabase = partitionedDatabase.get(superVertexId);
				if (existingDatabase == null) 
				{
					existingDatabase = new Database();
					partitionedDatabase.put(superVertexId, existingDatabase);
				}
				existingDatabase.addDataTable(tableName, tablePartition);
				existingDatabase.addDataTable("messages", messages);
			}
		}
		return partitionedDatabase;
	}

	public Map<SuperVertexId,Database> getDatabasesForEverySuperVertexWithMessages(Database inputDatabase, boolean isPagerank)
	{
		Map<SuperVertexId,Database> partitionedDatabase = new HashMap<>(); 
		Database relationalDatabase = getRelationalDatabase();
		for (Entry<String,Table> entry : relationalDatabase.tables.entrySet())
		{
			String tableName = entry.getKey();
			Table table = entry.getValue();
			
			Map<SuperVertexId,PartitionWithMessages> outgoingPartitionedTable = new HashMap<>();
			Map<SuperVertexId,PartitionWithMessages> incomingPartitionedTable = new HashMap<>();
			if (table.getRelationalType() == RelationalType.OUTGOING_RELATIONAL || 
					table.getRelationalType() == RelationalType.TWO_WAY_RELATIONAL)
				outgoingPartitionedTable = table.partitionWithMessages(inputDatabase.tables.get("outgoingNeighbors"), 
						inputDatabase.tables.get("neighborSuperVertices"), inputDatabase.tables.get("messages_full"), 
						inputDatabase.tables.get("incomingNeighbors"), isPagerank);
			
			if (table.getRelationalType() == RelationalType.INCOMING_RELATIONAL || 
					table.getRelationalType() == RelationalType.TWO_WAY_RELATIONAL)
				incomingPartitionedTable = table.partitionWithMessages(inputDatabase.tables.get("incomingNeighbors"),
						inputDatabase.tables.get("neighborSuperVertices"), inputDatabase.tables.get("messages_full"),
						inputDatabase.tables.get("outgoingNeighbors"), isPagerank);

			Map<SuperVertexId,PartitionWithMessages> partitionedTable = new HashMap<>();
			partitionedTable.putAll(outgoingPartitionedTable);
			partitionedTable.putAll(incomingPartitionedTable);
			
			for (Entry<SuperVertexId,PartitionWithMessages> partitionEntry : partitionedTable.entrySet())
			{
				SuperVertexId superVertexId = partitionEntry.getKey();
				Table tablePartition = partitionEntry.getValue().partition;
				Table messages = partitionEntry.getValue().messages;
				Database existingDatabase = partitionedDatabase.get(superVertexId);
				if (existingDatabase == null) 
				{
					existingDatabase = new Database();
					partitionedDatabase.put(superVertexId, existingDatabase);
				}
				existingDatabase.addDataTable(tableName, tablePartition);
				existingDatabase.addDataTable("messages", messages);
			}
		}
		return partitionedDatabase;
	}

	public Map<String,Integer> getTableSizes()
	{
		Map<String,Integer> tableSizes = new HashMap<>(); 
		for (Entry<String, Table> entry : tables.entrySet())
			tableSizes.put(entry.getKey(), entry.getValue().size());
		return tableSizes;
	}
	
	public boolean isEmpty()
	{
		return tables.isEmpty();
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("Database [tables=");
		for (Entry<String, Table> entry : tables.entrySet())
		{
			String tableName = entry.getKey();
			Table thisTable = entry.getValue();
			if (tableName.equals("vertices") || tableName.equals("edges") || tableName.equals("neighborSuperVertices") || tableName.equals("incomingNeighbors") || tableName.equals("outgoingNeighbors")) continue;
			s.append(tableName + "=" + thisTable);
		}
		s.append("]");
		return s.toString();
	}
	
}
