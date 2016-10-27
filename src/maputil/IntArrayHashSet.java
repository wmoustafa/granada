/* Generic definitions */
/* Assertions (useful to generate conditional code) */
/* Current type and class (and size, if applicable) */
/* Value methods */
/* Interfaces (keys) */
/* Interfaces (values) */
/* Abstract implementations (keys) */
/* Abstract implementations (values) */
/* Static containers (keys) */
/* Static containers (values) */
/* Implementations */
/* Synchronized wrappers */
/* Unmodifiable wrappers */
/* Other wrappers */
/* Methods (keys) */
/* Methods (values) */
/* Methods (keys/values) */
/* Methods that have special names depending on keys (but the special names depend on values) */
/* Equality */
/* Object/Reference-only definitions (keys) */
/* Object/Reference-only definitions (values) */
/*		 
 * Copyright (C) 2002-2015 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
package maputil;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** A type-specific hash set with with a fast, small-footprint implementation.
 *
 * <P>Instances of this class use a hash table to represent a set. The table is enlarged as needed by doubling its size when new entries are created, but it is <em>never</em> made smaller (even on a
 * {@link #clear()}). A family of {@linkplain #trim() trimming methods} lets you control the size of the table; this is particularly useful if you reuse instances of this class.
 *
 * @see Hash
 * @see HashCommon */
public class IntArrayHashSet extends AbstractObjectSet<int[]> implements java.io.Serializable, Cloneable, Hash {
	private static final long serialVersionUID = 0L;
	private static final boolean ASSERTS = false;
	/** The array of keys. */
	protected transient int[][] key;
	/** The mask for wrapping a position counter. */
	protected transient int mask;
	/** Whether this set contains the null key. */
	protected transient boolean containsNull;
	/** The current table size. */
	protected transient int n;
	/** Threshold after which we rehash. It must be the table size times {@link #f}. */
	protected transient int maxFill;
	/** Number of entries in the set (including the null key, if present). */
	protected int size;
	/** The acceptable load factor. */
	protected final float f;

	/** Creates a new hash set.
	 *
	 * <p>The actual table size will be the least power of two greater than <code>expected</code>/<code>f</code>.
	 *
	 * @param expected the expected number of elements in the hash set.
	 * @param f the load factor. */
	@SuppressWarnings("unchecked")
	public IntArrayHashSet( final int expected, final float f ) {
		if ( f <= 0 || f > 1 ) throw new IllegalArgumentException( "Load factor must be greater than 0 and smaller than or equal to 1" );
		if ( expected < 0 ) throw new IllegalArgumentException( "The expected number of elements must be nonnegative" );
		this.f = f;
		n = arraySize( expected, f );
		mask = n - 1;
		maxFill = maxFill( n, f );
		key = new int[ n + 1 ][];
	}

	/** Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
	 *
	 * @param expected the expected number of elements in the hash set. */
	public IntArrayHashSet( final int expected ) {
		this( expected, DEFAULT_LOAD_FACTOR );
	}

	/** Creates a new hash set with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} elements and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor. */
	public IntArrayHashSet() {
		this( DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR );
	}

	/** Creates a new hash set copying a given collection.
	 *
	 * @param c a {@link Collection} to be copied into the new hash set.
	 * @param f the load factor. */
	public IntArrayHashSet( final Collection<? extends int[]> c, final float f ) {
		this( c.size(), f );
		addAll( c );
	}

	/** Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given collection.
	 *
	 * @param c a {@link Collection} to be copied into the new hash set. */
	public IntArrayHashSet( final Collection<? extends int[]> c ) {
		this( c, DEFAULT_LOAD_FACTOR );
	}

	/** Creates a new hash set copying a given type-specific collection.
	 *
	 * @param c a type-specific collection to be copied into the new hash set.
	 * @param f the load factor. */
	public IntArrayHashSet( final ObjectCollection<? extends int[]> c, final float f ) {
		this( c.size(), f );
		addAll( c );
	}

	/** Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given type-specific collection.
	 *
	 * @param c a type-specific collection to be copied into the new hash set. */
	public IntArrayHashSet( final ObjectCollection<? extends int[]> c ) {
		this( c, DEFAULT_LOAD_FACTOR );
	}

	/** Creates a new hash set using elements provided by a type-specific iterator.
	 *
	 * @param i a type-specific iterator whose elements will fill the set.
	 * @param f the load factor. */
	public IntArrayHashSet( final Iterator<? extends int[]> i, final float f ) {
		this( DEFAULT_INITIAL_SIZE, f );
		while ( i.hasNext() )
			add( i.next() );
	}

	/** Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor using elements provided by a type-specific iterator.
	 *
	 * @param i a type-specific iterator whose elements will fill the set. */
	public IntArrayHashSet( final Iterator<? extends int[]> i ) {
		this( i, DEFAULT_LOAD_FACTOR );
	}

	/** Creates a new hash set and fills it with the elements of a given array.
	 *
	 * @param a an array whose elements will be used to fill the set.
	 * @param offset the first element to use.
	 * @param length the number of elements to use.
	 * @param f the load factor. */
	public IntArrayHashSet( final int[][] a, final int offset, final int length, final float f ) {
		this( length < 0 ? 0 : length, f );
		ObjectArrays.ensureOffsetLength( a, offset, length );
		for ( int i = 0; i < length; i++ )
			add( a[ offset + i ] );
	}

	/** Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor and fills it with the elements of a given array.
	 *
	 * @param a an array whose elements will be used to fill the set.
	 * @param offset the first element to use.
	 * @param length the number of elements to use. */
	public IntArrayHashSet( final int[][] a, final int offset, final int length ) {
		this( a, offset, length, DEFAULT_LOAD_FACTOR );
	}

	/** Creates a new hash set copying the elements of an array.
	 *
	 * @param a an array to be copied into the new hash set.
	 * @param f the load factor. */
	public IntArrayHashSet( final int[][] a, final float f ) {
		this( a, 0, a.length, f );
	}

	/** Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying the elements of an array.
	 *
	 * @param a an array to be copied into the new hash set. */
	public IntArrayHashSet( final int[][] a ) {
		this( a, DEFAULT_LOAD_FACTOR );
	}

	private int realSize() {
		return containsNull ? size - 1 : size;
	}

	private void ensureCapacity( final int capacity ) {
		final int needed = arraySize( capacity, f );
		if ( needed > n ) rehash( needed );
	}

	private void tryCapacity( final long capacity ) {
		final int needed = (int)Math.min( 1 << 30, Math.max( 2, HashCommon.nextPowerOfTwo( (long)Math.ceil( capacity / f ) ) ) );
		if ( needed > n ) rehash( needed );
	}

	/** {@inheritDoc} */
	public boolean addAll( Collection<? extends int[]> c ) {
		// The resulting collection will be at least c.size() big
		if ( f <= .5 ) ensureCapacity( c.size() ); // The resulting collection will be sized for c.size() elements
		else tryCapacity( size() + c.size() ); // The resulting collection will be tentatively sized for size() + c.size() elements
		return super.addAll( c );
	}

	public boolean add( final int[] h ) {
		int pos;
		if ( ( ( h ) == null ) ) {
			if ( containsNull ) return false;
			containsNull = true;
		}
		else {
			int length = h.length;
			final int[] k = new int[length]; 
			System.arraycopy(h, 0, k, 0, length);
			int[] curr;
			final int[][] key = this.key;
			// The starting point.
			if ( !( ( curr = key[ pos = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(k) ) ) & mask ] ) == null ) ) {
				if ( ( Arrays.equals(curr, k) ) ) return false;
				while ( !( ( curr = key[ pos = ( pos + 1 ) & mask ] ) == null ) )
					if ( ( Arrays.equals(curr, k) ) ) return false;
			}
			key[ pos ] = k;
		}
		if ( size++ >= maxFill ) rehash( arraySize( size + 1, f ) );
		if ( ASSERTS ) checkTable();
		return true;
	}

	/** Shifts left entries with the specified hash code, starting at the specified position, and empties the resulting free entry.
	 *
	 * @param pos a starting position. */
	protected final void shiftKeys( int pos ) {
		// Shift entries with the same hash.
		int last, slot;
		int[] curr;
		final int[][] key = this.key;
		for ( ;; ) {
			pos = ( ( last = pos ) + 1 ) & mask;
			for ( ;; ) {
				if ( ( ( curr = key[ pos ] ) == null ) ) {
					key[ last ] = ( null );
					return;
				}
				slot = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(curr) ) ) & mask;
				if ( last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos ) break;
				pos = ( pos + 1 ) & mask;
			}
			key[ last ] = curr;
		}
	}

	private boolean removeEntry( final int pos ) {
		size--;
		shiftKeys( pos );
		if ( size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE ) rehash( n / 2 );
		return true;
	}

	private boolean removeNullEntry() {
		containsNull = false;
		size--;
		if ( size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE ) rehash( n / 2 );
		return true;
	}

	public boolean remove( final int[] k ) {
		if ( ( ( k ) == null ) ) {
			if ( containsNull ) return removeNullEntry();
			return false;
		}
		int[] curr;
		final int[][] key = this.key;
		int pos;
		// The starting point.
		if ( ( ( curr = key[ pos = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(k) ) ) & mask ] ) == null ) ) return false;
		if ( ( Arrays.equals(curr, k) ) ) return removeEntry( pos );
		while ( true ) {
			if ( ( ( curr = key[ pos = ( pos + 1 ) & mask ] ) == null ) ) return false;
			if ( ( Arrays.equals(curr, k) ) ) return removeEntry( pos );
		}
	}

	public boolean contains( final int[] k ) {
		if ( ( ( k ) == null ) ) return containsNull;
		int[] curr;
		final int[][] key = this.key;
		int pos;
		// The starting point.
		if ( ( ( curr = key[ pos = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(k) ) ) & mask ] ) == null ) ) return false;
		if ( ( Arrays.equals(curr, k) ) ) return true;
		while ( true ) {
			if ( ( ( curr = key[ pos = ( pos + 1 ) & mask ] ) == null ) ) return false;
			if ( ( Arrays.equals(curr, k) ) ) return true;
		}
	}

	/** Returns the element of this set that is equal to the given key, or <code>null</code>.
	 * 
	 * @return the element of this set that is equal to the given key, or <code>null</code>. */

	public int[] get( final int[] k ) {
		if ( k == null ) return null; // This is correct independently of the value of containsNull
		int[] curr;
		final int[][] key = this.key;
		int pos;
		// The starting point.
		if ( ( ( curr = key[ pos = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(k) ) ) & mask ] ) == null ) ) return null;
		if ( ( Arrays.equals(curr, k) ) ) return curr;
		// There's always an unused entry.
		while ( true ) {
			if ( ( ( curr = key[ pos = ( pos + 1 ) & mask ] ) == null ) ) return null;
			if ( ( Arrays.equals(curr, k) ) ) return curr;
		}
	}

	/* Removes all elements from this set.
	 * 
	 * <P>To increase object reuse, this method does not change the table size. If you want to reduce the table size, you must use {@link #trim()}. */
	public void clear() {
		if ( size == 0 ) return;
		size = 0;
		containsNull = false;
		Arrays.fill( key, ( null ) );
	}

	public int size() {
		return size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	/** A no-op for backward compatibility.
	 * 
	 * @param growthFactor unused.
	 * @deprecated Since <code>fastutil</code> 6.1.0, hash tables are doubled when they are too full. */
	@Deprecated
	public void growthFactor( int growthFactor ) {}

	/** Gets the growth factor (2).
	 *
	 * @return the growth factor of this set, which is fixed (2).
	 * @see #growthFactor(int)
	 * @deprecated Since <code>fastutil</code> 6.1.0, hash tables are doubled when they are too full. */
	@Deprecated
	public int growthFactor() {
		return 16;
	}

	/** An iterator over a hash set. */
	private class SetIterator extends AbstractObjectIterator<int[]> {
		/** The index of the last entry returned, if positive or zero; initially, {@link #n}. If negative, the last element returned was that of index {@code - pos - 1} from the {@link #wrapped} list. */
		int pos = n;
		/** The index of the last entry that has been returned (more precisely, the value of {@link #pos} if {@link #pos} is positive, or {@link Integer#MIN_VALUE} if {@link #pos} is negative). It is
		 * -1 if either we did not return an entry yet, or the last returned entry has been removed. */
		int last = -1;
		/** A downward counter measuring how many entries must still be returned. */
		int c = size;
		/** A boolean telling us whether we should return the null key. */
		boolean mustReturnNull = IntArrayHashSet.this.containsNull;
		/** A lazily allocated list containing elements that have wrapped around the table because of removals. */
		ObjectArrayList<int[]> wrapped;

		public boolean hasNext() {
			return c != 0;
		}

		public int[] next() {
			if ( !hasNext() ) throw new NoSuchElementException();
			c--;
			if ( mustReturnNull ) {
				mustReturnNull = false;
				last = n;
				return ( null );
			}
			final int[] key[] = IntArrayHashSet.this.key;
			for ( ;; ) {
				if ( --pos < 0 ) {
					// We are just enumerating elements from the wrapped list.
					last = Integer.MIN_VALUE;
					return wrapped.get( -pos - 1 );
				}
				if ( !( ( key[ pos ] ) == null ) ) return key[ last = pos ];
			}
		}

		/** Shifts left entries with the specified hash code, starting at the specified position, and empties the resulting free entry.
		 *
		 * @param pos a starting position. */
		private final void shiftKeys( int pos ) {
			// Shift entries with the same hash.
			int last, slot;
			int[] curr;
			final int[][] key = IntArrayHashSet.this.key;
			for ( ;; ) {
				pos = ( ( last = pos ) + 1 ) & mask;
				for ( ;; ) {
					if ( ( ( curr = key[ pos ] ) == null ) ) {
						key[ last ] = ( null );
						return;
					}
					slot = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(curr) ) ) & mask;
					if ( last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos ) break;
					pos = ( pos + 1 ) & mask;
				}
				if ( pos < last ) { // Wrapped entry.
					if ( wrapped == null ) wrapped = new ObjectArrayList<int[]>( 2 );
					wrapped.add( key[ pos ] );
				}
				key[ last ] = curr;
			}
		}

		public void remove() {
			if ( last == -1 ) throw new IllegalStateException();
			if ( last == n ) IntArrayHashSet.this.containsNull = false;
			else if ( pos >= 0 ) shiftKeys( last );
			else {
				// We're removing wrapped entries.
				IntArrayHashSet.this.remove( wrapped.set( -pos - 1, null ) );
				last = -1; // Note that we must not decrement size
				return;
			}
			size--;
			last = -1; // You can no longer remove this entry.
			if ( ASSERTS ) checkTable();
		}
	}

	public ObjectIterator<int[]> iterator() {
		return new SetIterator();
	}

	/** A no-op for backward compatibility. The kind of tables implemented by this class never need rehashing.
	 *
	 * <P>If you need to reduce the table size to fit exactly this set, use {@link #trim()}.
	 *
	 * @return true.
	 * @see #trim()
	 * @deprecated A no-op. */
	@Deprecated
	public boolean rehash() {
		return true;
	}

	/** Rehashes this set, making the table as small as possible.
	 * 
	 * <P>This method rehashes the table to the smallest size satisfying the load factor. It can be used when the set will not be changed anymore, so to optimize access speed and size.
	 *
	 * <P>If the table size is already the minimum possible, this method does nothing.
	 *
	 * @return true if there was enough memory to trim the set.
	 * @see #trim(int) */
	public boolean trim() {
		final int l = arraySize( size, f );
		if ( l >= n ) return true;
		try {
			rehash( l );
		}
		catch ( OutOfMemoryError cantDoIt ) {
			return false;
		}
		return true;
	}

	/** Rehashes this set if the table is too large.
	 * 
	 * <P>Let <var>N</var> be the smallest table size that can hold <code>max(n,{@link #size()})</code> entries, still satisfying the load factor. If the current table size is smaller than or equal to
	 * <var>N</var>, this method does nothing. Otherwise, it rehashes this set in a table of size <var>N</var>.
	 *
	 * <P>This method is useful when reusing sets. {@linkplain #clear() Clearing a set} leaves the table size untouched. If you are reusing a set many times, you can call this method with a typical
	 * size to avoid keeping around a very large table just because of a few large transient sets.
	 *
	 * @param n the threshold for the trimming.
	 * @return true if there was enough memory to trim the set.
	 * @see #trim() */
	public boolean trim( final int n ) {
		final int l = HashCommon.nextPowerOfTwo( (int)Math.ceil( n / f ) );
		if ( this.n <= l ) return true;
		try {
			rehash( l );
		}
		catch ( OutOfMemoryError cantDoIt ) {
			return false;
		}
		return true;
	}

	/** Rehashes the set.
	 *
	 * <P>This method implements the basic rehashing strategy, and may be overriden by subclasses implementing different rehashing strategies (e.g., disk-based rehashing). However, you should not
	 * override this method unless you understand the internal workings of this class.
	 *
	 * @param newN the new size */
	@SuppressWarnings("unchecked")
	protected void rehash( final int newN ) {
		final int[] key[] = this.key;
		final int mask = newN - 1; // Note that this is used by the hashing macro
		final int[] newKey[] = new int[ newN + 1 ][];
		int i = n, pos;
		for ( int j = realSize(); j-- != 0; ) {
			while ( ( ( key[ --i ] ) == null ) );
			if ( !( ( newKey[ pos = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(key[ i ]) ) ) & mask ] ) == null ) ) while ( !( ( newKey[ pos = ( pos + 1 ) & mask ] ) == null ) );
			newKey[ pos ] = key[ i ];
		}
		n = newN;
		this.mask = mask;
		maxFill = maxFill( n, f );
		this.key = newKey;
	}

	/** Returns a deep copy of this set.
	 *
	 * <P>This method performs a deep copy of this hash set; the data stored in the set, however, is not cloned. Note that this makes a difference only for object keys.
	 *
	 * @return a deep copy of this set. */
	@SuppressWarnings("unchecked")
	public IntArrayHashSet clone() {
		IntArrayHashSet c;
		try {
			c = (IntArrayHashSet)super.clone();
		}
		catch ( CloneNotSupportedException cantHappen ) {
			throw new InternalError();
		}
		c.key = key.clone();
		c.containsNull = containsNull;
		return c;
	}

	/** Returns a hash code for this set.
	 *
	 * This method overrides the generic method provided by the superclass. Since <code>equals()</code> is not overriden, it is important that the value returned by this method is the same value as
	 * the one returned by the overriden method.
	 *
	 * @return a hash code for this set. */
	public int hashCode() {
		int h = 0;
		for ( int j = realSize(), i = 0; j-- != 0; ) {
			while ( ( ( key[ i ] ) == null ) )
				i++;
			h += ( Arrays.hashCode(key[ i ]) );
			i++;
		}
		// Zero / null have hash zero.
		return h;
	}

	private void writeObject( java.io.ObjectOutputStream s ) throws java.io.IOException {
		final ObjectIterator<int[]> i = iterator();
		s.defaultWriteObject();
		for ( int j = size; j-- != 0; )
			s.writeObject( i.next() );
	}

	@SuppressWarnings("unchecked")
	private void readObject( java.io.ObjectInputStream s ) throws java.io.IOException, ClassNotFoundException {
		s.defaultReadObject();
		n = arraySize( size, f );
		maxFill = maxFill( n, f );
		mask = n - 1;
		final int[] key[] = this.key = new int[ n + 1 ][];
		int[] k;
		for ( int i = size, pos; i-- != 0; ) {
			k = (int[])s.readObject();
			if ( ( ( k ) == null ) ) {
				pos = n;
				containsNull = true;
			}
			else {
				if ( !( ( key[ pos = ( it.unimi.dsi.fastutil.HashCommon.mix( Arrays.hashCode(k) ) ) & mask ] ) == null ) ) while ( !( ( key[ pos = ( pos + 1 ) & mask ] ) == null ) );
				key[ pos ] = k;
			}
		}
		if ( ASSERTS ) checkTable();
	}

	private void checkTable() {}
	
}