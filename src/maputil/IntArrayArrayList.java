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
/* int[]/Reference-only definitions (keys) */
/* int[]/Reference-only definitions (values) */
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.RandomAccess;

import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.AbstractObjectListIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;

import java.util.NoSuchElementException;

/** A type-specific array-based list; provides some additional methods that use polymorphism to avoid (un)boxing.
 *
 * <P>This class implements a lightweight, fast, open, optimized, reuse-oriented version of array-based lists. Instances of this class represent a list with an array that is enlarged as needed when
 * new entries are created (by doubling the current length), but is <em>never</em> made smaller (even on a {@link #clear()}). A family of {@linkplain #trim() trimming methods} lets you control the size
 * of the backing array; this is particularly useful if you reuse instances of this class. Range checks are equivalent to those of {@link java.util}'s classes, but they are delayed as much as
 * possible.
 *
 * <p>The backing array is exposed by the {@link #elements()} method. If an instance of this class was created {@linkplain #wrap(int[][],int) by wrapping}, backing-array reallocations will be
 * performed using reflection, so that {@link #elements()} can return an array of the same type of the original array: the comments about efficiency made in
 * {@link it.unimi.dsi.fastutil.objects.ObjectArrays} apply here. Moreover, you must take into consideration that assignment to an array not of type {@code int[][]} is slower due to type checking.
 *
 * <p>This class implements the bulk methods <code>removeElements()</code>, <code>addElements()</code> and <code>getElements()</code> using high-performance system calls (e.g.,
 * {@link System#arraycopy(int[],int,int[],int,int) System.arraycopy()} instead of expensive loops.
 *
 * @see java.util.ArrayList */
public class IntArrayArrayList extends AbstractObjectList<int[]> implements RandomAccess, Cloneable, java.io.Serializable {
	private static final long serialVersionUID = -7046029254386353131L;
	/** The initial default capacity of an array list. */
	public final static int DEFAULT_INITIAL_CAPACITY = 16;
	/** Whether the backing array was passed to <code>wrap()</code>. In this case, we must reallocate with the same type of array. */
	protected final boolean wrapped;
	/** The backing array. */
	protected transient int[][] a;
	/** The current actual size of the list (never greater than the backing-array length). */
	protected int size;
	private static final boolean ASSERTS = false;

	/** Creates a new array list using a given array.
	 *
	 * <P>This constructor is only meant to be used by the wrapping methods.
	 *
	 * @param a the array that will be used to back this array list. */
	@SuppressWarnings("unused")
	protected IntArrayArrayList( final int[][] a, boolean dummy ) {
		this.a = a;
		this.wrapped = true;
	}

	/** Creates a new array list with given capacity.
	 *
	 * @param capacity the initial capacity of the array list (may be 0). */
	@SuppressWarnings("unchecked")
	public IntArrayArrayList( final int capacity ) {
		if ( capacity < 0 ) throw new IllegalArgumentException( "Initial capacity (" + capacity + ") is negative" );
		a = (int[][])new int[ capacity ][];
		wrapped = false;
	}

	/** Creates a new array list with {@link #DEFAULT_INITIAL_CAPACITY} capacity. */
	public IntArrayArrayList() {
		this( DEFAULT_INITIAL_CAPACITY );
	}

	/** Creates a new array list and fills it with a given collection.
	 *
	 * @param c a collection that will be used to fill the array list. */
	public IntArrayArrayList( final Collection<? extends int[]> c ) {
		this( c.size() );
		size = ObjectIterators.unwrap( c.iterator(), a );
	}

	/** Creates a new array list and fills it with a given type-specific collection.
	 *
	 * @param c a type-specific collection that will be used to fill the array list. */
	public IntArrayArrayList( final ObjectCollection<? extends int[]> c ) {
		this( c.size() );
		size = ObjectIterators.unwrap( c.iterator(), a );
	}

	/** Creates a new array list and fills it with a given type-specific list.
	 *
	 * @param l a type-specific list that will be used to fill the array list. */
	public IntArrayArrayList( final ObjectList<? extends int[]> l ) {
		this( l.size() );
		l.getElements( 0, a, 0, size = l.size() );
	}

	/** Creates a new array list and fills it with the elements of a given array.
	 *
	 * @param a an array whose elements will be used to fill the array list. */
	public IntArrayArrayList( final int[][] a ) {
		this( a, 0, a.length );
	}

	/** Creates a new array list and fills it with the elements of a given array.
	 *
	 * @param a an array whose elements will be used to fill the array list.
	 * @param offset the first element to use.
	 * @param length the number of elements to use. */
	public IntArrayArrayList( final int[][] a, final int offset, final int length ) {
		this( length );
		System.arraycopy( a, offset, this.a, 0, length );
		size = length;
	}

	/** Creates a new array list and fills it with the elements returned by an iterator..
	 *
	 * @param i an iterator whose returned elements will fill the array list. */
	public IntArrayArrayList( final Iterator<? extends int[]> i ) {
		this();
		while ( i.hasNext() )
			this.add( i.next() );
	}

	/** Creates a new array list and fills it with the elements returned by a type-specific iterator..
	 *
	 * @param i a type-specific iterator whose returned elements will fill the array list. */
	public IntArrayArrayList( final ObjectIterator<? extends int[]> i ) {
		this();
		while ( i.hasNext() )
			this.add( i.next() );
	}

	/** Returns the backing array of this list.
	 *
	 * <P>If this array list was created by wrapping a given array, it is guaranteed that the type of the returned array will be the same. Otherwise, the returned array will be of type {@link int[]
	 * int[][]} (in spite of the declared return type).
	 * 
	 * <strong>Warning</strong>: This behaviour may cause (unfathomable) run-time errors if a method expects an array actually of type <code>int[][]</code>, but this methods returns an array of type
	 * {@link int[] int[][]}.
	 *
	 * @return the backing array. */
	public int[][] elements() {
		return a;
	}

	/** Wraps a given array into an array list of given size.
	 *
	 * @param a an array to wrap.
	 * @param length the length of the resulting array list.
	 * @return a new array list of the given size, wrapping the given array. */
	public static IntArrayArrayList wrap( final int[][] a, final int length ) {
		if ( length > a.length ) throw new IllegalArgumentException( "The specified length (" + length + ") is greater than the array size (" + a.length + ")" );
		final IntArrayArrayList l = new IntArrayArrayList( a, false );
		l.size = length;
		return l;
	}

	/** Wraps a given array into an array list.
	 *
	 * @param a an array to wrap.
	 * @return a new array list wrapping the given array. */
	public static IntArrayArrayList wrap( final int[][] a ) {
		return wrap( a, a.length );
	}

	/** Ensures that this array list can contain the given number of entries without resizing.
	 *
	 * @param capacity the new minimum capacity for this array list. */
	@SuppressWarnings("unchecked")
	public void ensureCapacity( final int capacity ) {
		if ( wrapped ) a = ObjectArrays.ensureCapacity( a, capacity, size );
		else {
			if ( capacity > a.length ) {
				final int[][] t = new int[ capacity ][];
				System.arraycopy( a, 0, t, 0, size );
				a = (int[][])t;
			}
		}
		if ( ASSERTS ) assert size <= a.length;
	}

	/** Grows this array list, ensuring that it can contain the given number of entries without resizing, and in case enlarging it at least by a factor of two.
	 *
	 * @param capacity the new minimum capacity for this array list. */
	@SuppressWarnings("unchecked")
	private void grow( final int capacity ) {
		if ( wrapped ) a = ObjectArrays.grow( a, capacity, size );
		else {
			if ( capacity > a.length ) {
				final int newLength = (int)Math.max( Math.min( 2L * a.length, it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ), capacity );
				final int[][] t = new int[ newLength ][];
				System.arraycopy( a, 0, t, 0, size );
				a = (int[][])t;
			}
		}
		if ( ASSERTS ) assert size <= a.length;
	}

	public void add( final int index, final int[] k ) {
		ensureIndex( index );
		grow( size + 1 );
		if ( index != size ) System.arraycopy( a, index, a, index + 1, size - index );
		int length = k.length;
		int[] h = new int[length];
		System.arraycopy(k, 0, h, 0, length);
		a[ index ] = h;
		size++;
		if ( ASSERTS ) assert size <= a.length;
	}

	public boolean add( final int[] k ) {
		grow( size + 1 );
		int length = k.length;
		int[] h = new int[length];
		System.arraycopy(k, 0, h, 0, length);
		a[ size++ ] = h;
		if ( ASSERTS ) assert size <= a.length;
		return true;
	}

	public int[] get( final int index ) {
		if ( index >= size ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to list size (" + size + ")" );
		return a[ index ];
	}

	public int indexOf( final int[] k ) {
		for ( int i = 0; i < size; i++ )
			if ( ( ( k ) == null ? ( a[ i ] ) == null : Arrays.equals(k, a[ i ] ) ) ) return i;
		return -1;
	}

	public int lastIndexOf( final int[] k ) {
		for ( int i = size; i-- != 0; )
			if ( ( ( k ) == null ? ( a[ i ] ) == null : Arrays.equals(k, a[ i ] ) ) ) return i;
		return -1;
	}

	public int[] remove( final int index ) {
		if ( index >= size ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to list size (" + size + ")" );
		final int[] old = a[ index ];
		size--;
		if ( index != size ) System.arraycopy( a, index + 1, a, index, size - index );
		a[ size ] = null;
		if ( ASSERTS ) assert size <= a.length;
		return old;
	}

	public boolean rem( final int[] k ) {
		int index = indexOf( k );
		if ( index == -1 ) return false;
		remove( index );
		if ( ASSERTS ) assert size <= a.length;
		return true;
	}

	public boolean remove( final int[] o ) {
		return rem( o );
	}

	public int[] set( final int index, final int[] k ) {
		if ( index >= size ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to list size (" + size + ")" );
		int[] old = a[ index ];
		a[ index ] = k;
		return old;
	}

	public void clear() {
		Arrays.fill( a, 0, size, null );
		size = 0;
		if ( ASSERTS ) assert size <= a.length;
	}

	public int size() {
		return size;
	}

	public void size( final int size ) {
		if ( size > a.length ) ensureCapacity( size );
		if ( size > this.size ) Arrays.fill( a, this.size, size, ( null ) );
		else Arrays.fill( a, size, this.size, ( null ) );
		this.size = size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	/** Trims this array list so that the capacity is equal to the size.
	 *
	 * @see java.util.ArrayList#trimToSize() */
	public void trim() {
		trim( 0 );
	}

	/** Trims the backing array if it is too large.
	 * 
	 * If the current array length is smaller than or equal to <code>n</code>, this method does nothing. Otherwise, it trims the array length to the maximum between <code>n</code> and {@link #size()}.
	 *
	 * <P>This method is useful when reusing lists. {@linkplain #clear() Clearing a list} leaves the array length untouched. If you are reusing a list many times, you can call this method with a
	 * typical size to avoid keeping around a very large array just because of a few large transient lists.
	 *
	 * @param n the threshold for the trimming. */
	@SuppressWarnings("unchecked")
	public void trim( final int n ) {
		// TODO: use Arrays.trim() and preserve type only if necessary
		if ( n >= a.length || size == a.length ) return;
		final int[][] t = (int[][])new int[ Math.max( n, size ) ][];
		System.arraycopy( a, 0, t, 0, size );
		a = t;
		if ( ASSERTS ) assert size <= a.length;
	}

	/** Copies element of this type-specific list into the given array using optimized system calls.
	 *
	 * @param from the start index (inclusive).
	 * @param a the destination array.
	 * @param offset the offset into the destination array where to store the first element copied.
	 * @param length the number of elements to be copied. */
	public void getElements( final int from, final int[][] a, final int offset, final int length ) {
		ObjectArrays.ensureOffsetLength( a, offset, length );
		System.arraycopy( this.a, from, a, offset, length );
	}

	/** Removes elements of this type-specific list using optimized system calls.
	 *
	 * @param from the start index (inclusive).
	 * @param to the end index (exclusive). */
	public void removeElements( final int from, final int to ) {
		it.unimi.dsi.fastutil.Arrays.ensureFromTo( size, from, to );
		System.arraycopy( a, to, a, from, size - to );
		size -= ( to - from );
		int i = to - from;
		while ( i-- != 0 )
			a[ size + i ] = null;
	}

	/** Adds elements to this type-specific list using optimized system calls.
	 *
	 * @param index the index at which to add elements.
	 * @param a the array containing the elements.
	 * @param offset the offset of the first element to add.
	 * @param length the number of elements to add. */
	public void addElements( final int index, final int[][] a, final int offset, final int length ) {
		ensureIndex( index );
		ObjectArrays.ensureOffsetLength( a, offset, length );
		grow( size + length );
		System.arraycopy( this.a, index, this.a, index + length, size - index );
		System.arraycopy( a, offset, this.a, index, length );
		size += length;
	}

	public ObjectListIterator<int[]> listIterator( final int index ) {
		ensureIndex( index );
		return new AbstractObjectListIterator<int[]>() {
			int pos = index, last = -1;

			public boolean hasNext() {
				return pos < size;
			}

			public boolean hasPrevious() {
				return pos > 0;
			}

			public int[] next() {
				if ( !hasNext() ) throw new NoSuchElementException();
				return a[ last = pos++ ];
			}

			public int[] previous() {
				if ( !hasPrevious() ) throw new NoSuchElementException();
				return a[ last = --pos ];
			}

			public int nextIndex() {
				return pos;
			}

			public int previousIndex() {
				return pos - 1;
			}

			public void add( int[] k ) {
				if ( last == -1 ) throw new IllegalStateException();
				IntArrayArrayList.this.add( pos++, k );
				last = -1;
			}

			public void set( int[] k ) {
				if ( last == -1 ) throw new IllegalStateException();
				IntArrayArrayList.this.set( last, k );
			}

			public void remove() {
				if ( last == -1 ) throw new IllegalStateException();
				IntArrayArrayList.this.remove( last );
				/* If the last operation was a next(), we are removing an element *before* us, and we must decrease pos correspondingly. */
				if ( last < pos ) pos--;
				last = -1;
			}
		};
	}

	public IntArrayArrayList clone() {
		IntArrayArrayList c = new IntArrayArrayList( size );
		System.arraycopy( a, 0, c.a, 0, size );
		c.size = size;
		return c;
	}

	private boolean valEquals( final int[] a, final int[] b ) {
		return a == null ? b == null : Arrays.equals(a, b );
	}

	/** Compares this type-specific array list to another one.
	 *
	 * <P>This method exists only for sake of efficiency. The implementation inherited from the abstract implementation would already work.
	 *
	 * @param l a type-specific array list.
	 * @return true if the argument contains the same elements of this type-specific array list. */
	public boolean equals( final IntArrayArrayList l ) {
		if ( l == this ) return true;
		int s = size();
		if ( s != l.size() ) return false;
		final int[][] a1 = a;
		final int[][] a2 = l.a;
		while ( s-- != 0 )
			if ( !valEquals( a1[ s ], a2[ s ] ) ) return false;
		return true;
	}

	private void writeObject( java.io.ObjectOutputStream s ) throws java.io.IOException {
		s.defaultWriteObject();
		for ( int i = 0; i < size; i++ )
			s.writeObject( a[ i ] );
	}

	@SuppressWarnings("unchecked")
	private void readObject( java.io.ObjectInputStream s ) throws java.io.IOException, ClassNotFoundException {
		s.defaultReadObject();
		a = (int[][])new int[ size ][];
		for ( int i = 0; i < size; i++ )
			a[ i ] = (int[])s.readObject();
	}
}
