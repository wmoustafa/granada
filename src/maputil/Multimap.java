package maputil;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

public class Multimap<Integer,Tuple> {
	
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
	public ObjectSet<Entry<java.lang.Integer, LinkedList<Tuple>>> entries() {
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
