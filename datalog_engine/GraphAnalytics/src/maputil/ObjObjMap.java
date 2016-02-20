package maputil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import schema.Tuple;

/**
 * Object-2-object map based on IntIntMap4a
 */
public class ObjObjMap
{
    private static final Tuple FREE_KEY = null;

    /** Keys and values */
    private Tuple[] keys;
    private LinkedList<Tuple>[] values;

    /** Fill factor, must be between (0 and 1) */
    private final float m_fillFactor;
    /** We will resize a map once it reaches this size */
    private int m_threshold;
    /** Current map size */
    private int m_size;
    /** Mask to calculate the original position */
    private int m_mask;
    /** Mask to wrap the actual array pointer */
    //private int m_mask2;

    public ObjObjMap( final int size, final float fillFactor )
    {
        if ( fillFactor <= 0 || fillFactor >= 1 )
            throw new IllegalArgumentException( "FillFactor must be in (0, 1)" );
        if ( size <= 0 )
            throw new IllegalArgumentException( "Size must be positive!" );
        final int capacity = arraySize(size, fillFactor);
        m_mask = capacity - 1;
        //m_mask2 = capacity * 2 - 1;
        m_fillFactor = fillFactor;

        keys = new Tuple[capacity];
        values = new LinkedList[capacity];

        m_threshold = (int) (capacity * fillFactor);
    }

    public LinkedList get( final Tuple key )
    {
    	int i = 1;
        int ptr = (key.hashCode() & m_mask);
        Tuple k = keys[ ptr ];

        if ( k == FREE_KEY )
            return null;  //end of chain already
        if ( k.equals( key ) ) //we check FREE and REMOVED prior to this call
            return values[ ptr ];
        while ( true )
        {
        	i ++;
            ptr = (ptr + 1) & m_mask; //that's next index
            k = keys[ ptr ];
            if ( k == FREE_KEY )
                { if (Math.random()<0.005) System.out.println(i + " NULL " + ptr); return null;}
            if ( k.equals( key ) )
            	{ if (Math.random()<0.005) System.out.println(i + " " + key + " " + ptr); return values[ ptr ];}
        }
    }

    public LinkedList put( final Tuple key, final LinkedList value )
    {
        int ptr = getStartIndex(key);
        Tuple k = keys[ptr];

        if ( k == FREE_KEY ) //end of chain already
        {
            keys[ ptr ] = key;
            values[ ptr ] = value;
            if ( m_size >= m_threshold )
                rehash( keys.length * 2 ); //size is set inside
            else
                ++m_size;
            return null;
        }
        else if ( k.equals( key ) ) //we check FREE and REMOVED prior to this call
        {
            final LinkedList ret = values[ ptr ];
            values[ ptr ] = value;
            return ret;
        }

        while ( true )
        {
            ptr = ( ptr + 1 ) & m_mask; //that's next index calculation
            k = keys[ ptr ];
            if ( k == FREE_KEY )
            {
                keys[ ptr ] = key;
                values[ ptr ] = value;
                if ( m_size >= m_threshold )
                    rehash( keys.length * 2 ); //size is set inside
                else
                    ++m_size;
                return null;
            }
            else if ( k.equals( key ) )
            {
                final LinkedList ret = values[ ptr ];
                values[ ptr ] = value;
                return ret;
            }
        }
    }


    public int size()
    {
        return m_size;
    }

    private void rehash( final int newCapacity )
    {
        m_threshold = (int) (newCapacity * m_fillFactor);
        m_mask = newCapacity - 1;
        //m_mask2 = newCapacity - 1;

        final int oldCapacity = keys.length;
        final Tuple[] oldKeys = keys;
        final LinkedList[] oldValues = values;

        keys = new Tuple[ newCapacity ];
        values = new LinkedList[ newCapacity ];

        m_size = 0;

        for ( int i = 0; i < oldCapacity; i += 1 ) {
            final Tuple oldKey = oldKeys[ i ];
            final LinkedList oldValue = oldValues[ i ];
            if( oldKey != FREE_KEY )
                put(oldKey, oldValue);
        }
    }
    
    public Iterator<LinkedList> valueIterator()
    {
		    	return new Iterator<LinkedList>() {
		    		int currentKeyIndex = 0;
		    		Tuple currentKey;
		    		LinkedList currentValue;

					@Override
					public boolean hasNext() {
						for (; currentKeyIndex < keys.length;)
						{
							currentKey = keys[currentKeyIndex];
							currentValue = values[currentKeyIndex];
							currentKeyIndex += 1;
							if (currentKey != FREE_KEY ) return true;
						}
						return false;
					}

					@Override
					public LinkedList next() {
						return currentValue;
					}

					@Override
					public void remove() {				
					}
				};
    	
    }

    public Iterable<LinkedList> valueIterable()
    {
    	return new Iterable<LinkedList>() {

			@Override
			public Iterator<LinkedList> iterator() {
				// TODO Auto-generated method stub
		    	return new Iterator<LinkedList>() {
		    		int currentKeyIndex = 0;
		    		Tuple currentKey;
		    		LinkedList currentValue;

					@Override
					public boolean hasNext() {
						for (; currentKeyIndex < keys.length;)
						{
							currentKey = keys[currentKeyIndex];
							currentValue = values[currentKeyIndex];
							currentKeyIndex += 1;
							if (currentKey != FREE_KEY ) return true;
						}
						return false;
					}

					@Override
					public LinkedList next() {
						return currentValue;
					}

					@Override
					public void remove() {				
					}
				};
			}
		};
    	
    }

    public int getStartIndex( final Tuple key )
    {
        //key is not null here
        return key.hashCode() & m_mask;
    }
    
    public String toString()
    {
    	return Arrays.toString(keys);
    }
    
    static int arraySize( final int expected, final float f ) {
   		final long s = Math.max( 2, nextPowerOfTwo( (long)Math.ceil( expected / f ) ) );
   		if ( s > (1 << 30) ) throw new IllegalArgumentException( "Too large (" + expected + " expected elements with load factor " + f + ")" );
   		return (int)s;
   	}
    
   	static long nextPowerOfTwo( long x ) {
   		if ( x == 0 ) return 1;
   		x--;
   		x |= x >> 1;
   		x |= x >> 2;
   		x |= x >> 4;
   		x |= x >> 8;
   		x |= x >> 16;
   		return ( x | x >> 32 ) + 1;
   	}

   	class Entry {
   		Tuple key;
   		LinkedList<Tuple> value;
   	}
}