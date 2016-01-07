/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.giraph.graph;

import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.SimplePartition;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by Vicky Papavasileiou on 8/19/15.
 */
public class OrderedPartition<I extends WritableComparable,
        V extends Writable, E extends Writable> extends SimplePartition<I,V,E> implements Iterable<Vertex<I, V, E>>{

    private static final Logger LOG  = Logger.getLogger(OrderedPartition.class);
    private ArrayKey[] ordered_array;
    private int source_partition = -1;
    private List<Vertex> samples;

    //private TreeMap<Double,HashMap<I,Vertex>> ordered_map;

    private Partition<I, V, E> partition;

    public OrderedPartition(Partition<I, V, E> partition)
    {
        this.partition = partition;
        ordered_array = new ArrayKey[10];
        samples = new ArrayList<Vertex>(10);
        //ordered_map = new TreeMap<Double,HashMap<I,Vertex>>(10);
    }

    public void setSourcePartition(int id) {
     this.source_partition = id;
    }


    private void sample_vertices() {
        int counter = 0;
        for(Vertex vertex : partition) {
            if(((DoubleWritable)vertex.getValue()).get() < Double.MAX_VALUE && counter < 10) {
                samples.add(vertex);
                counter++;
            }
        }
    }

    /**
     * Get first 10 vertices and add them to the array ordered by the vertex value.
     * Add every other vertex to the corresponding bucket using binary search
     */
    public void initialize_array() {

        sample_vertices();
        int counter = 0;
        for(Vertex vertex: samples) {
            ArrayKey key = new ArrayKey(((DoubleWritable)vertex.getValue()), new LinkedList<>());
            ordered_array[counter] = key;
            key.getVertices().add(vertex);
            counter++;
        }
        Iterator<Vertex<I, V, E>> it = partition.iterator();

        while(it.hasNext()) {
            Vertex vertex = it.next();
            if(!samples.contains(vertex)) {
                if (counter < 10) {

                    ArrayKey key = new ArrayKey(((DoubleWritable) vertex.getValue()), new LinkedList<>());
                    ordered_array[counter] = key;
                    key.getVertices().add(vertex);
                } else if (counter >= 10) {
                    if (counter == 10) {
                        Arrays.sort(ordered_array);
                    }

                    ArrayKey key = new ArrayKey(((DoubleWritable) vertex.getValue()));
                    int result = java.util.Arrays.binarySearch(ordered_array, key);
                    //LOG.info(" result = " + result + " complement = " + (~result));

                    if (result < 0) {
                        //LOG.info("Vicky -->  Lookup and add vertex " + (DoubleWritable) vertex.getValue() + " at " + (~result));
                        if (~result >= 10)
                            ordered_array[9].vertices.add(vertex);
                        else {
                            ordered_array[~result].vertices.add(vertex);
                        }
                    } else {
                        ordered_array[result].vertices.add(vertex);

                    }
                }
                counter++;
            }
        }

    }

    public String printSizes() {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<10; i++) {
            sb.append("List at index " + i + " size = " + ordered_array[i].getVertices().size() + "\n");
        }
        return sb.toString();
    }

    public String printSourcePartition() {
        StringBuilder sb = new StringBuilder();
        if(partition.getId() == source_partition) {
            for(int i= 0; i<10; i++) {
                sb.append(ordered_array[i].getVertices().toString());
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i= 0; i<10; i++) {
            sb.append("Vertices at index " + i + ":: " + ordered_array[i].getVertices().toString() + "\n");
        }
        return sb.toString();
    }


    @Override
    public Iterator<Vertex<I, V, E>> iterator() {
        return new OrderedVertexIterator();
    }


    private class OrderedVertexIterator implements Iterator<Vertex<I, V, E>> {
        private int cursor;
        private ListIterator<Vertex> inner_iterator;
        private int inner_cursor = 0;

        public OrderedVertexIterator() {
            this.cursor = 0;
            inner_iterator = ordered_array[0].getVertices().listIterator(0);
        }

        public boolean hasNext() {
            //LOG.info("hasNext cursor = " + cursor);
            //LOG.info("hasNext inner_cursor = " + inner_cursor);
            if( this.cursor < ordered_array.length-1)
                return true;
            else if(this.cursor == (ordered_array.length-1) && inner_cursor < ordered_array[this.cursor].vertices.size()) {
                inner_cursor++;
                return true;
            }
            return false;
            //return this.cursor < ordered_array.length ;
        }

        public Vertex<I, V, E> next() {
            //LOG.info("Vicky --> cursor " + cursor);
            if(inner_iterator.hasNext()) {
                //LOG.info("Inner iterator has next");
                return inner_iterator.next();
            }
            else if(this.hasNext() && !inner_iterator.hasNext()) {
                //LOG.info("Inner iterator has NO next");
                cursor++;
                //LOG.info("Cursor after increment " + cursor);
                if(this.hasNext()) {
                    inner_iterator = ordered_array[cursor].getVertices().listIterator(0);
                    return inner_iterator.next();
                }

            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    class ArrayKey<V extends Writable> implements Comparable{
        V value;
        LinkedList<Vertex<I, V, E>> vertices;

        public ArrayKey(V value, LinkedList<Vertex<I, V, E>> vertices) {
            this.value = value;
            this.vertices = vertices;
        }

        public ArrayKey(V value) {
            this.value = value;
            this.vertices = null;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public LinkedList<Vertex<I, V, E>> getVertices() {
            return vertices;
        }

        public void setVertices(LinkedList< Vertex<I, V, E>> vertices) {
            this.vertices = vertices;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArrayKey<?> arrayKey = (ArrayKey<?>) o;

            return value.equals(arrayKey.value);

        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public int compareTo(Object o) {
            if (this == o) return 0;
            if (o == null || getClass() != o.getClass()) return -1;

            ArrayKey<?> arrayKey = (ArrayKey<?>) o;

            return ((DoubleWritable)this.value).compareTo((DoubleWritable)arrayKey.getValue());

       }
    }

}
