package maputil;

import java.util.Iterator;
import java.util.LinkedList;

import schema.Tuple;

public class ObjObjMultimap<K,V> {
	
	ObjObjMap map;
	int size = 0;
	
	public ObjObjMultimap()
	{
		map = new ObjObjMap(16, 0.75f);
	}
	
	public void put(Tuple key, Tuple value)
	{
		LinkedList<Tuple> existingSet = map.get(key);
		if (existingSet == null)
		{
			existingSet = new LinkedList<>();
			map.put(key, existingSet);
		}
		boolean elementAdded = existingSet.add(value);
		if (elementAdded) size++;
	}
	
	public void remove(Tuple key, Tuple value)
	{
		LinkedList<Tuple> existingSet = map.get(key);
		if (existingSet != null)
		{
			boolean removed = existingSet.remove(value);
			if (removed) size--;
			//if (existingSet.isEmpty()) map.remove(key);
		}
	}

	public LinkedList<Tuple> get(Tuple key)
	{
		LinkedList<Tuple> value = map.get(key);
		if (value != null) return value; else return new LinkedList<>();
	}
	
	public boolean contains(Tuple key, Tuple value)
	{
		LinkedList<Tuple> values = map.get(key);
		if (values == null) return false;
		return values.contains(value);
	}
	
	public int size()
	{
		return size;
	}
	
	public Iterable<Tuple> values()
	{
		final Iterator<LinkedList> values= map.valueIterator();
		return new Iterable<Tuple>() {

			@Override
			public Iterator<Tuple> iterator() {

				return new Iterator<Tuple>() {
					
					private Iterator<Tuple> current = values.hasNext() ? values.next().iterator() : null;
						
					@Override
					public boolean hasNext() {
						while (true)
						{
							if (current == null) return false;
							if (current.hasNext()) return true;
							current = values.hasNext() ? values.next().iterator() : null;
						}
					}

					@Override
					public Tuple next() {
						return current.next();
					}

					@Override
					public void remove() {
						// TODO Auto-generated method stub
						
					}
				};
			}
		};
	}
	
	

}
