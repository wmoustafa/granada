package maputil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import schema.Tuple;

public class Multimap {
	
//	ModifiedJavaHashmap<K,LinkedList<V>> map;
	LinkedList<Tuple> emptyValue = new LinkedList<>();
	int size = 0;
	Int2ObjectOpenHashMap<LinkedList<Tuple>> map;
	
	public Multimap()
	{
		map = new Int2ObjectOpenHashMap<LinkedList<Tuple>>();
	}
	
	public Multimap(int initialSize)
	{
		map = new Int2ObjectOpenHashMap<LinkedList<Tuple>>(); 
	}

	public void put(int key, Tuple value)
	{
		LinkedList<Tuple> existingSet = map.get(key);
		if (existingSet == null)
		{
			existingSet = new LinkedList<Tuple>();
			map.put(key, existingSet);
		}
		boolean elementAdded = existingSet.add(value);
		if (elementAdded) size++;
	}
	
	public void remove(int key, Tuple value)
	{
		LinkedList<Tuple> existingSet = map.get(key);
		if (existingSet != null)
		{
			boolean removed = existingSet.remove(value);
			if (removed) size--;
			//if (existingSet.isEmpty()) map.remove(key);
		}
	}

	public LinkedList<Tuple> get(int key)
	{
		LinkedList<Tuple> value = map.get(key);
		if (value != null) return value; else return emptyValue;
	}
	
	public boolean contains(int key, Tuple value)
	{
		LinkedList<Tuple> values = map.get(key);
		if (values == null) return false;
		return values.contains(value);
	}
	
	public int size()
	{
		return size;
	}
	
	/**
	 * This method is expensive as it traverses through the map and adds the sizes of each LinkedList.
	 * @return The size of the map including the sizes of the LinkedList per bucket of the map.
	 */
	public int getSizeRecursively()
	{
		int size = 0;
		for(Integer key: map.keySet())
		{
			size+=map.get(key).size();
		}
		return size;
	}
	
	
	public Set<Entry<java.lang.Integer, LinkedList<Tuple>>> entries() {
		return map.entrySet();
	}
	public Iterable<Tuple> values()
	{
		final Iterator<LinkedList<Tuple>> values= map.values().iterator();
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
