package maputil;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class GoogleMultimap<K extends Comparable,V extends Comparable> {
	
	Multimap<K,V> map;
	
	public GoogleMultimap()
	{
		map = HashMultimap.create();
	}
	
	public void put(K key, V value)
	{
		map.put(key, value);
	}
	
	public void remove(K key, V value)
	{
		map.remove(key, value);
	}

	public Collection<V> get(K key)
	{
		return map.get(key);
	}
	
	public boolean contains(K key, V value)
	{
		return map.containsEntry(key, value);
	}
	
	public int size()
	{
		return map.entries().size();
	}
	
	public Iterable<V> values()
	{
		return map.values();
	}
	
	

}
