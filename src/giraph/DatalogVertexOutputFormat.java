package giraph;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import schema.Database;
import schema.Table;
import schema.Tuple;

public class DatalogVertexOutputFormat extends TextVertexOutputFormat<SuperVertexId, Database, NullWritable> {


	  @Override
	  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
	    return new IdWithValueVertexWriter();
	  }

	  /**
	   * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	   */
	  protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
	    /** Saved delimiter */
	    private String delimiter = "\t";

	    
	    @Override
	    protected Text convertVertexToLine(Vertex<SuperVertexId, Database, NullWritable> vertex) 
	    		throws IOException {

	      StringBuilder str = new StringBuilder();
	      
	      //Print each vertex in a super-vertex and it's value
	      // Essentially, we print the contents of the *_full table
	      
	      Table full_table = vertex.getValue().getDataTableByName("wcc_full"); //FIXME 
	      assert(full_table != null);
	      
	      for(int[] t: full_table.getData().values())
	      {
	    	  str.append(t[0]);
	    	  str.append(delimiter);
	    	  str.append(t[1]);
	    	  str.append("\n");
	      }
	      	     
	      return new Text(str.toString());
	    }
	  }

}