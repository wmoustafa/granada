package giraph;

import org.apache.giraph.graph.VertexValueCombiner;

import schema.Database;

public class DatalogVertexCombiner implements VertexValueCombiner<Database>{

	@Override
	public void combine(Database db1, Database db2) {
		db1.combine(db2);
	}

}
