package maputil;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

public class Multimap {
	
//	ModifiedJavaHashmap<K,LinkedList<V>> map;
	IntArrayHashSet emptyValue = new IntArrayHashSet(1);
	int size = 0;
	Int2ObjectOpenHashMap<IntArrayHashSet> map;
	
	public Multimap()
	{
		map = new Int2ObjectOpenHashMap<IntArrayHashSet>();
	}
	
	public Multimap(int initialSize)
	{
		map = new Int2ObjectOpenHashMap<IntArrayHashSet>(); 
	}

	public void put(int key, int[] value)
	{
		IntArrayHashSet existingSet = map.get(key);
		if (existingSet == null)
		{
			existingSet = new IntArrayHashSet(1);
			map.put(key, existingSet);
		}
		boolean elementAdded = existingSet.add(value);
		if (elementAdded) size++;
	}
	
	public void remove(int key, int[] value)
	{
		IntArrayHashSet existingSet = map.get(key);
		if (existingSet != null)
		{
			boolean removed = existingSet.remove(value);
			if (removed) size--;
			//if (existingSet.isEmpty()) map.remove(key);
		}
	}

	public IntArrayHashSet get(int key)
	{
		IntArrayHashSet value = map.get(key);
		if (value != null) return value; else return emptyValue;
	}
	
	public boolean contains(int key, int[] value)
	{
		IntArrayHashSet values = map.get(key);
		if (values == null) return false;
		return values.contains(value);
	}
	
	public int size()
	{
		return size;
	}
	public ObjectSet<Entry<java.lang.Integer, IntArrayHashSet>> entries() {
		return map.entrySet();
	}
	public Iterable<int[]> values()
	{
		final Iterator<IntArrayHashSet> values= map.values().iterator();
		return new Iterable<int[]>() {

			@Override
			public Iterator<int[]> iterator() {

				return new Iterator<int[]>() {
					
					private Iterator<int[]> current = values.hasNext() ? values.next().iterator() : null;
						
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
					public int[] next() {
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
