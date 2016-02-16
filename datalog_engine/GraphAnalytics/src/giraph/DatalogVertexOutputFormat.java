package giraph;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import schema.Database;

public class DatalogVertexOutputFormat extends TextVertexOutputFormat<SuperVertexId, Database, NullWritable> {
	
	@Override
	  public TextVertexWriter createVertexWriter(TaskAttemptContext tac) {
	    return new IdWithOutputTableVertexWriter();
	  }

	  
	  class IdWithOutputTableVertexWriter extends TextVertexWriterToEachLine {
	   
	    
	    @Override
	    protected Text convertVertexToLine(Vertex<SuperVertexId, Database, NullWritable> vertex) throws IOException {

	      StringBuilder str = new StringBuilder();
	     
	        str.append(vertex.getId().toString());
	        str.append(" ");
	        str.append(vertex.getValue().getDataTableByName("wcc_full").toString());
//	        str.append(vertex.getValue().toString());
	        
	      return new Text(str.toString());
	    }
	  }

}
