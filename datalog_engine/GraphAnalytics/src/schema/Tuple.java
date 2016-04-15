package schema;

import java.io.Serializable;
import java.util.Arrays;

public class Tuple implements Serializable {
	
//	Value key;
//	Value[] values;
	int[] values;

	public Tuple(int[] fields)
	{
		this.values = fields;
	}
	
	public int[] getFields()
	{
		return values;
	}
		
	public int[] toArray()
	{
		return values;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(values);
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Tuple)) return false;
		return Arrays.equals(values, ((Tuple)obj).values);
	}
	
	public String toString()
	{
		return Arrays.toString(values);
	}

}
