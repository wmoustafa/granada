package giraph;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import objectexplorer.MemoryMeasurer;
import schema.Database;
import schema.Metadata;
import schema.Table;
import schema.Tuple;
import utils.AggregationFunctionType;



public class DatalogVertexInputFormatFromEachLine extends TextVertexInputFormat<SuperVertexId, Database, NullWritable>{

	private static final Logger LOG =
		      Logger.getLogger(DatalogVertexInputFormatFromEachLine.class);

	@Override
	public TextVertexInputFormat<SuperVertexId, Database, NullWritable>.TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new RelationalVertexReaderFromEachLine();
	}
	
	class RelationalVertexReaderFromEachLine extends TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException>
	{

		@Override
		protected Iterable<Edge<SuperVertexId, NullWritable>> getEdges(
				JSONArray jsonVertex) throws JSONException, IOException {

			//JSONArray jsonSuperVertexEdges = jsonVertex.getJSONArray(2);
			List<Edge<SuperVertexId, NullWritable>> edges = new ArrayList<Edge<SuperVertexId, NullWritable>>();			
			/*for (int i = 0; i < jsonSuperVertexEdges.length(); i++)
			{
				JSONArray jsonOtherVertexArray = jsonSuperVertexEdges.getJSONArray(i);
				SuperVertexId otherVertex = new SuperVertexId((short)(jsonOtherVertexArray.getInt(0)), jsonOtherVertexArray.getInt(1));
				Edge<SuperVertexId, NullWritable> edge = EdgeFactory.create(otherVertex);
				edges.add(edge);
			}*/
			return edges;
		}

		@Override
		protected SuperVertexId getId(JSONArray jsonVertex) throws JSONException,
				IOException {
			JSONArray superVertexId = jsonVertex.getJSONArray(0);
			return new SuperVertexId((short)(superVertexId.getInt(0)), superVertexId.getInt(1));
		}

		@Override
		protected Database getValue(JSONArray jsonVertex)
				throws JSONException, IOException {
			
			Metadata metadata = new Metadata();
			
			////System.out.println("************************************************");
			JSONArray jsonSuperVertexValues = jsonVertex.getJSONArray(1);
			
			int[] vertexKeyFields = new int[]{0};
			//Class[] vertexFieldTypes = new Class[]{Integer.class, String.class, String.class};
			Class[] vertexFieldTypes = new Class[]{Integer.class, Integer.class};
			Table vertexTable = new Table(vertexFieldTypes, vertexKeyFields, jsonSuperVertexValues.length());
			long vertex_number = 0;
			long in_edges_number = 0;
			long out_edges_number = 0;
			long super_out_edges = 0;
			long super_in_edges = 0;

			int nEdges = 0;
			for (int i = 0; i < jsonSuperVertexValues.length(); i++)
			{
				JSONArray jsonVertexValues = jsonSuperVertexValues.getJSONArray(i);
				JSONArray jsonInEdgeTupleArray = jsonVertexValues.getJSONArray(1);
				JSONArray jsonOutEdgeTupleArray = jsonVertexValues.getJSONArray(2);

				nEdges += jsonInEdgeTupleArray.length() + jsonOutEdgeTupleArray.length();
				jsonVertexValues = null;
				jsonInEdgeTupleArray = null;
				jsonOutEdgeTupleArray = null;
			}
			
			int[] edgeKeyFields = new int[]{0};
			Class[] edgeFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
//			Table edgesTable = new Table(edgeFieldTypes, edgeKeyFields, nEdges);

			int[] outgoingNeighborsKeyFields = new int[]{0};
			Class[] outgoingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
			Table outgoingNeighborsTable = new Table(outgoingNeighborsFieldTypes, outgoingNeighborsKeyFields, jsonSuperVertexValues.length());

			int[] incomingNeighborsKeyFields = new int[]{0};
			Class[] incomingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
//			Table incomingNeighborsTable = new Table(incomingNeighborsFieldTypes, incomingNeighborsKeyFields, jsonSuperVertexValues.length());

//			int[] messagesKeyFields = new int[]{0};
//			Class[] messagesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
//			Table messagesTable = new Table(messagesFieldTypes, messagesKeyFields);
//			messagesTable.setAggregationFunctionType(AggregationFunctionType.SUM);;

			JSONArray jsonNeighborSuperVertices = jsonVertex.getJSONArray(3);

			int[] neighborSuperVerticesKeyFields = new int[]{0};
			//Class[] vertexFieldTypes = new Class[]{Integer.class, String.class, String.class};
			Class[] neighborSuperVerticesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
			Table neighborSuperVerticesTable = new Table(neighborSuperVerticesFieldTypes, neighborSuperVerticesKeyFields, jsonNeighborSuperVertices.length());
			
			////System.out.println(jsonSuperVertexValues.length() + " " + nEdges + " " + jsonNeighborSuperVertices.length());
			////System.out.println("Free memory: " + Runtime.getRuntime().freeMemory()/1024/1024);
			long t1 = System.currentTimeMillis();
			for (int i = 0; i < jsonSuperVertexValues.length(); i++)
			{
				JSONArray jsonVertexValues = jsonSuperVertexValues.getJSONArray(i);
				JSONArray jsonVertexTuple = jsonVertexValues.getJSONArray(0);
				JSONArray jsonInEdgeTupleArray = jsonVertexValues.getJSONArray(1);
				JSONArray jsonOutEdgeTupleArray = jsonVertexValues.getJSONArray(2);

				int[] vertexTuple = new int[jsonVertexTuple.length()];
				for (int j = 0; j < jsonVertexTuple.length(); j++)
				{
					if (vertexFieldTypes[j] == String.class) 
						throw new RuntimeException("String: Unsupported data type");
					else if (vertexFieldTypes[j] == Integer.class) 
						vertexTuple[j] = jsonVertexTuple.getInt(j);
					else if (vertexFieldTypes[j] == Boolean.class) 
						throw new RuntimeException("Boolean: Unsupported data type");
				}				
				vertexTable.putTuple(new Tuple(vertexTuple));
				vertex_number++;

//				int[] messagesTuple = new int[3];
//				messagesTuple[0] = jsonVertexTuple.getInt(0);
//				messagesTuple[1] = 0;
//				messagesTuple[2] = 0;
//				messagesTable.putTuple(new Tuple(messagesTuple));
//				for (int j = 0; j < jsonInEdgeTupleArray.length(); j++)
//				{
//					int[] edgeTuple = new int[3];
//					edgeTuple[1] = jsonVertexTuple.getInt(0);
//					JSONArray edgeEndVertexAndWeight = jsonInEdgeTupleArray.getJSONArray(j);
//					edgeTuple[0] = edgeEndVertexAndWeight.getInt(0);
//					edgeTuple[2] = edgeEndVertexAndWeight.getInt(1);
////					edgesTable.putTuple(new Tuple(edgeTuple));
//					in_edges_number++;
//
//					int[] neighborTuple = new int[3];
//					neighborTuple[0] = jsonVertexTuple.getInt(0);
//					neighborTuple[1] = edgeEndVertexAndWeight.getInt(0);
//					neighborTuple[2] = edgeEndVertexAndWeight.getInt(1);
//					incomingNeighborsTable.putTuple(new Tuple(neighborTuple));
//					edgeEndVertexAndWeight = null;					
//				}

				for (int j = 0; j < jsonOutEdgeTupleArray.length(); j++)
				{
					int[] edgeTuple = new int[3];
					edgeTuple[0] = jsonVertexTuple.getInt(0);
					JSONArray edgeEndVertexAndWeight = jsonOutEdgeTupleArray.getJSONArray(j);
					edgeTuple[1] = edgeEndVertexAndWeight.getInt(0);
					edgeTuple[2] = edgeEndVertexAndWeight.getInt(1);
//					edgesTable.putTuple(new Tuple(edgeTuple));
					out_edges_number++;
					
					outgoingNeighborsTable.putTuple(new Tuple(edgeTuple));
					edgeEndVertexAndWeight = null;
				}
				jsonVertexValues = null;
				jsonVertexTuple = null;
				jsonInEdgeTupleArray = null;
				jsonOutEdgeTupleArray = null;
			}
			long t2 = System.currentTimeMillis();
			////System.out.println(t2-t1);
			long t3 = System.currentTimeMillis();

						
			for (int i = 0; i < jsonNeighborSuperVertices.length(); i++)
			{
				JSONArray jsonNeighborSuperVertexTuple = jsonNeighborSuperVertices.getJSONArray(i);
				int[] neighborSuperVertexTuple = new int[3];
				neighborSuperVertexTuple[0] = jsonNeighborSuperVertexTuple.getInt(0);
				neighborSuperVertexTuple[1] = jsonNeighborSuperVertexTuple.getInt(1);
				neighborSuperVertexTuple[2] = jsonNeighborSuperVertexTuple.getInt(2);
				neighborSuperVerticesTable.putTuple(new Tuple(neighborSuperVertexTuple));
				jsonNeighborSuperVertexTuple = null;
				super_out_edges++;
			}
			long t4 = System.currentTimeMillis();
			////System.out.println(t4-t2);

			metadata.setMetadata("vertices", vertexKeyFields, vertexFieldTypes);
			metadata.setMetadata("edges", edgeKeyFields, edgeFieldTypes);
			metadata.setMetadata("incomingNeighbors", incomingNeighborsKeyFields, incomingNeighborsFieldTypes);
			metadata.setMetadata("outgoingNeighbors", outgoingNeighborsKeyFields, outgoingNeighborsFieldTypes);
			
//			System.out.println("Metadata after reading input " + metadata);
			
			Database database = new Database(metadata,-1);
			StringBuffer sb = new StringBuffer();
			database.addDataTable("vertices", vertexTable);
//			sb.append("[Size of vertices = " + MemoryMeasurer.measureBytes(vertexTable) + "]");
			database.addDataTable("outgoingNeighbors", outgoingNeighborsTable);
//			sb.append("[database after out neigh = " + MemoryMeasurer.measureBytes(database) + "].");
			database.addDataTable("neighborSuperVertices", neighborSuperVerticesTable);
//			sb.append("[database after neighbor super = " + MemoryMeasurer.measureBytes(database) + "].");
			

			//FIXME the tables below are not needed for the evaluation
//			database.addDataTable("edges", edgesTable); //TODO Vicky checking if can be removed
//			sb.append("[database after edges = " + MemoryMeasurer.measureBytes(database) + "].");
//			database.addDataTable("incomingNeighbors", incomingNeighborsTable); //TODO Vicky checking if can be removed
//			sb.append("[database after in neigh = " + MemoryMeasurer.measureBytes(database) + "].");
//			database.addDataTable("messages_full", messagesTable);
//			sb.append("[database after msg table = " + MemoryMeasurer.measureBytes(database) + "].");
			
			

			sb.append("[Size of  database  = " + MemoryMeasurer.measureBytes(database) + "].");
			System.out.println(sb.toString());
			
			return database;
		}

		@Override
		protected JSONArray preprocessLine(Text line) throws JSONException,
				IOException {
			return new JSONArray(line.toString());
		}
		
	}

}