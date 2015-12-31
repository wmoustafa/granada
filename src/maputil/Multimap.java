package maputil;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Multimap<K,V> {
	
	ModifiedJavaHashmap<K,LinkedList<V>> map;
	LinkedList<V> emptyValue = new LinkedList<>();
	int size = 0;
	
	public Multimap()
	{
		map = new ModifiedJavaHashmap<K,LinkedList<V>>(16, 0.75f);
	}
	
	public Multimap(int initialSize)
	{
		map = new ModifiedJavaHashmap<K,LinkedList<V>>(initialSize, 0.75f);
	}

	public void put(K key, V value)
	{
		LinkedList<V> existingSet = map.get(key);
		if (existingSet == null)
		{
			existingSet = new LinkedList<>();
			map.put(key, existingSet);
		}
		boolean elementAdded = existingSet.add(value);
		if (elementAdded) size++;
	}
	
	public void remove(K key, V value)
	{
		LinkedList<V> existingSet = map.get(key);
		if (existingSet != null)
		{
			boolean removed = existingSet.remove(value);
			if (removed) size--;
			//if (existingSet.isEmpty()) map.remove(key);
		}
	}

	public LinkedList<V> get(K key)
	{
		LinkedList<V> value = map.get(key);
		if (value != null) return value; else return emptyValue;
	}
	
	public boolean contains(K key, V value)
	{
		LinkedList<V> values = map.get(key);
		if (values == null) return false;
		return values.contains(value);
	}
	
	public int size()
	{
		return size;
	}
	
	public Iterable<V> values()
	{
		final Iterator<LinkedList<V>> values= map.valueIterator();
		return new Iterable<V>() {

			@Override
			public Iterator<V> iterator() {

				return new Iterator<V>() {
					
					private Iterator<V> current = values.hasNext() ? values.next().iterator() : null;
						
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
					public V next() {
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
