package giraph;

import schema.Database;

public class DatalogMessageCombiner 
//extends MessageCombiner<SuperVertexId, Database> 
{

	public void combine(SuperVertexId id, Database db1, Database db2) {
		db1.combine2(db2);
	}

//	public Database createInitialMessage() {
//		return new Database();
//	}

}
