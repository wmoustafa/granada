package maputil;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class Multimap<Integer,Tuple> {
	
//	ModifiedJavaHashmap<K,LinkedList<V>> map;
	ArrayList<Tuple> emptyValue = new ArrayList<>();
	int size = 0;
	Int2ObjectOpenHashMap<ArrayList<Tuple>> map;
	
	public Multimap()
	{
		map = new Int2ObjectOpenHashMap<ArrayList<Tuple>>();
	}
	
	public Multimap(int initialSize)
	{
		map = new Int2ObjectOpenHashMap<ArrayList<Tuple>>(initialSize); 
	}

	public void put(int key, Tuple value)
	{
		ArrayList<Tuple> existingSet = map.get(key);
		if (existingSet == null)
		{
			existingSet = new ArrayList<Tuple>();
			map.put(key, existingSet);
		}
		boolean elementAdded = existingSet.add(value);
		if (elementAdded) size++;
	}
	
	public void remove(int key, Tuple value)
	{
		ArrayList<Tuple> existingSet = map.get(key);
		if (existingSet != null)
		{
			boolean removed = existingSet.remove(value);
			if (removed) size--;
			//if (existingSet.isEmpty()) map.remove(key);
		}
	}

	public ArrayList<Tuple> get(int key)
	{
		ArrayList<Tuple> value = map.get(key);
		if (value != null) return value; else return emptyValue;
	}
	
	public boolean contains(int key, Tuple value)
	{
		ArrayList<Tuple> values = map.get(key);
		if (values == null) return false;
		return values.contains(value);
	}
	
	public int size()
	{
		return size;
	}
	
	public Iterable<Tuple> values()
	{
		final Iterator<ArrayList<Tuple>> values= map.values().iterator();
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
