package giraph;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import schema.Database;
import schema.Metadata;
import schema.Table;

	public class DatalogVertexInputFormatNoJSON extends 
			TextVertexInputFormat<SuperVertexId, Database, NullWritable>{
	
		private AtomicInteger counter = new AtomicInteger(0);

		@Override
		public TextVertexInputFormat<SuperVertexId, Database, NullWritable>.TextVertexReader createVertexReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException {
			return new DatalogVertexReaderFromEachLine();
		}
		
		public class DatalogVertexReaderFromEachLine extends 
				TextVertexReaderFromEachLineProcessedHandlingExceptions<String, IOException>
		{

			@Override
			protected Iterable<Edge<SuperVertexId, NullWritable>> getEdges(String input) {
				
				//No edges between super-vertices
				List<Edge<SuperVertexId, NullWritable>> edges = new ArrayList<Edge<SuperVertexId, NullWritable>>();			
				return edges;
			}

			@Override
			public SuperVertexId getId(String input) throws IOException {
				Pattern id = Pattern.compile("\\[\\[(\\d+?,\\d+?)\\]");			
				Matcher m = id.matcher(input);
				if(!m.find())	
				{
					System.out.println("---> No match found for " + input.substring(0, 100));
					System.out.println("Read " + counter.get() + " lines");					
//					throw new IOException("The input string did not match the regex pattern for super-vertex id."
//							+ "Input = " + input.substring(0, 100));
					return new SuperVertexId((short)-1, -1);
				}	
				String[] sv_id = m.group(1).split(",");
//				System.out.println(Integer.parseInt(sv_id[0])+","+ Integer.parseInt(sv_id[1]));
				return new SuperVertexId((short)Integer.parseInt(sv_id[0]), Integer.parseInt(sv_id[1]));
			}

			@Override
			public Database getValue(String input) throws  IOException {								
				
				Metadata metadata = new Metadata();
															
				Pattern sv_data = Pattern.compile("(\\[.*?\\])(\\[.*?\\])\\]");
				Pattern vdata = Pattern.compile("\\((\\d+,\\d+)\\)(\\[.*?\\])");
				Pattern edges = Pattern.compile("\\((\\d+,\\d+)\\)");
				Pattern sv_edges = Pattern.compile("\\((\\d+,\\d+,\\d+)\\)");
				Matcher sv_matcher = sv_data.matcher(input.substring(6));
				Matcher v_matcher = vdata.matcher("");
				Matcher e_matcher = edges.matcher("");
				Matcher sve_matcher = sv_edges.matcher("");
				int numVertices = 0;
				int numSuperNeighbors = 0;
				
				if(sv_matcher.find())
				{
					v_matcher.reset(sv_matcher.group(1));
			
					//Read vertex data
					int[] vertexTuple = new int[2];
					while(v_matcher.find())
						numVertices++;
					
					//Read neighbors of current super-vertex
					sve_matcher.reset(sv_matcher.group(2));
					while(sve_matcher.find())
						numSuperNeighbors++;
				}

				int[] vertexKeyFields = new int[]{0};
				Class[] vertexFieldTypes = new Class[]{Integer.class, Integer.class};
				Table vertexTable = new Table(vertexFieldTypes, vertexKeyFields, numVertices); // <<- FIXME Initial size

				int[] outgoingNeighborsKeyFields = new int[]{0};
				Class[] outgoingNeighborsFieldTypes = new Class[]{Integer.class, Integer.class};
				Table outgoingNeighborsTable = new Table(outgoingNeighborsFieldTypes, outgoingNeighborsKeyFields, numVertices); // <<- FIXME Initial size

				int[] neighborSuperVerticesKeyFields = new int[]{0};
				Class[] neighborSuperVerticesFieldTypes = new Class[]{Integer.class, Integer.class, Integer.class};
				Table neighborSuperVerticesTable = new Table(neighborSuperVerticesFieldTypes, neighborSuperVerticesKeyFields, numSuperNeighbors);

				sv_matcher.reset();
				if(sv_matcher.find())
				{
					v_matcher.reset(sv_matcher.group(1));
			
					//Read vertex data
					int[] vertexTuple = new int[2];
					while(v_matcher.find())
						{
						//Read vertex id
						String[] v_id = v_matcher.group(1).split(",");
						vertexTuple[0] = Integer.parseInt(v_id[0]);
						vertexTuple[1] = Integer.parseInt(v_id[1]);
						vertexTable.putTuple(vertexTuple);
						
						//Read edges of current vertex
						e_matcher.reset(v_matcher.group(2));
						int numEdges = 0;
						while(e_matcher.find())
							numEdges++;
						e_matcher.reset(v_matcher.group(2));
						int[] edgeTuple = new int[2];
						while(e_matcher.find())
						{
							edgeTuple[0] = vertexTuple[0];
							String[] e_id = e_matcher.group(1).split(",");
							edgeTuple[1] = Integer.parseInt(e_id[0]);
							outgoingNeighborsTable.putTuple(edgeTuple);
						}
					}
					
					//Read neighbors of current super-vertex
					sve_matcher.reset(sv_matcher.group(2));
					int[] neighborSuperVertexTuple = new int[3];
					while(sve_matcher.find())
					{
						String[] e_id = sve_matcher.group(1).split(",");
						neighborSuperVertexTuple[0] = Integer.parseInt(e_id[0]);
						neighborSuperVertexTuple[1] = Integer.parseInt(e_id[1]);
						neighborSuperVertexTuple[2] = Integer.parseInt(e_id[2]);
						neighborSuperVerticesTable.putTuple(neighborSuperVertexTuple);
					}
				}
				else
				{
					throw new IOException("The input string did not match the regex pattern for vertex data."
							+ "Input = " + input);
				}
			
				metadata.setMetadata("vertices", vertexKeyFields, vertexFieldTypes);
				metadata.setMetadata("outgoingNeighbors", outgoingNeighborsKeyFields, outgoingNeighborsFieldTypes);
//				if(input.charAt(2) == '0' && input.charAt(4) == '0')
//				{
//					System.out.println((input.charAt(2) - '0')+","+ (input.charAt(4) - '0'));
//					System.out.println("vertices = " + vertexTable);
//					System.out.println("edges" + outgoingNeighborsTable);
//				}
//				System.out.println("sv neighbors" + neighborSuperVerticesTable);
				
				vertexTable.finalize();
				outgoingNeighborsTable.finalize();
				neighborSuperVerticesTable.finalize();

				Database database = new Database(metadata,-1);
				database.addDataTable("vertices", vertexTable);
				database.addDataTable("outgoingNeighbors", outgoingNeighborsTable);
				database.addDataTable("neighborSuperVertices", neighborSuperVerticesTable);
				
//				Date date = new Date();
//				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//				String formattedDate = sdf.format(date);
//				System.out.println(formattedDate + "  Finished loading graph \n.");
				
//				System.out.println("[Size of  database  = " + MemoryMeasurer.measureBytes(database) + "].");
				
				return database;
			}

			@Override
			protected String preprocessLine(Text line) throws IOException {
				System.out.println(line.toString().substring(0, 5));
				System.out.println("Length of line = " + line.toString().length());
				System.out.println("Read " + counter.get() + " lines");
				counter.addAndGet(1);
				return (line.toString());
			}
			
		}

	}
