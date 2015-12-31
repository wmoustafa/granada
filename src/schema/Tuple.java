package schema;

import java.io.Serializable;
import java.util.Arrays;

public class Tuple implements Serializable, Comparable<Tuple> {
	
	Object[] values;

	public Tuple(Object[] fields)
	{
		this.values = fields;
	}
	
	public Object[] getFields()
	{
		return values;
	}
		
	public Object[] toArray()
	{
		return values;
	}
	
	@Override
	public int hashCode() {
		int result = values[0].hashCode();
		for (int i = 1; i < values.length; i++)
			result = 31 * result + values[i].hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		final Object[] otherValues = ((Tuple)obj).values;
		for (int i = 0; i < values.length; i++)
			if (!values[i].equals(otherValues[i])) return false;
		return true;
	}
	
	public int compareTo(Tuple otherValues)
	{
		for (int i=0; i<values.length; i++)
			if (!values[i].equals(otherValues.values[i]))
				if (values[i] instanceof Integer)
					return ((Integer)values[i]).compareTo((Integer)otherValues.values[i]);
				else if (values[i] instanceof String)
					return ((String)values[i]).compareTo((String)otherValues.values[i]);
		return 0;
	}
	
	public String toString()
	{
		return Arrays.deepToString(values);
	}

}
