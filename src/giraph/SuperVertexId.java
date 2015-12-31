package giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SuperVertexId implements WritableComparable<SuperVertexId> {
	
	int vertexId;
	short partitionId;

	@Override
	public int hashCode() {
		return vertexId;
	}

	@Override
	public boolean equals(Object obj) {
		SuperVertexId other = (SuperVertexId) obj;
		if (vertexId != other.vertexId)
			return false;
		return true;
	}
	
	
	public SuperVertexId() {
		super();
		this.vertexId = -1;
		this.partitionId = -1;
	}

	public SuperVertexId(short partitionId, int vertexId) {
		super();
		this.vertexId = vertexId;
		this.partitionId = partitionId;
	}
	
	public int getVertexId() {
		return vertexId;
	}
	
	public short getPartitionId() {
		return partitionId;
	}
		
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		vertexId = in.readInt();
		partitionId = in.readShort();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(vertexId);
		out.writeShort(partitionId);
	}

	@Override
	public int compareTo(SuperVertexId o) {
		// TODO Auto-generated method stub
		return vertexId - o.vertexId;
	}
	
	public String toString()
	{
		return "[" + partitionId + "," + vertexId + "]";
	}

}
