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

package org.apache.giraph.comm.messages.primitives.long_id;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by Vicky Papavasileiou on 7/17/15.
 */
public class SenderIdIterationNumber<I extends WritableComparable> implements Comparable, Writable{


    // Vicky
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(SenderIdIterationNumber.class);


    /**The Id of the vertex that sent the message */
    protected I senderId;
    /** The sender's worker iteration number  */
    protected Long iteration;

    protected Set<I> neighbors;

    protected Map<Long,Boolean> map;

    /**
     * Constructor
     */
    public SenderIdIterationNumber(I senderId, Long iteration) {
        this.senderId = senderId;
        this.iteration = iteration;
        //this.neighbors = new HashSet<I>();
        initialize_map();
    }

    public SenderIdIterationNumber() {
        initialize_map();
    }


    public I getSenderId(){ return this.senderId; }

    public Long getIteration(){
        return this.iteration;
    }

    public Set<I> getNeighbors() { return this.neighbors; }

    public Map<Long,Boolean> getMap() { return  this.map; }

    public boolean isMessageReadByNeighbor(Long vid)
    {
        return map.get(vid);
    }

    public void setSenderId(I senderId) {
        this.senderId = senderId;
    }

    public void setIteration(Long iteration){
        this.iteration = iteration;
    }

    public void setNeighbors(Set<I> neighbors) {
        this.neighbors = neighbors;
    }

    public void addNeighbor(I neighbor) { this.neighbors.add(neighbor); }

    public void setMessageReadByNeighbor(Long vid)
    {

        this.map.put(vid, true);
    }

    public void markUnreadByNeighbor(Long vid)
    {
        this.map.put(vid,false);
    }

    public void initialize_map()
    {
        map = new HashMap<Long,Boolean>();
        /*for(I vid: neighbors) {
            map.put(vid, false);
        }*/
    }

    @Override
    public String toString(){
        String str = senderId + ", " + iteration + " : " + map.toString();
        return str;
    }


    @Override
    public int compareTo(Object o) {
        if(!(o instanceof SenderIdIterationNumber))
            throw new RuntimeException("SenderIdIterationNumber comparator: Not an instance of this class");
        return (((I)this.senderId).compareTo(((I)((SenderIdIterationNumber)o).senderId)));
    }

    @Override
    public boolean equals(Object o) {
      if(!(o instanceof SenderIdIterationNumber))
        return false;
      if(this == o) {
          return true;
      }
      if(this.senderId == (((SenderIdIterationNumber)o).senderId)) {
          return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * this.senderId.hashCode();
      return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        senderId.write(dataOutput);
        dataOutput.writeLong(iteration);
        //TODO Don't send neighbors nor map for increased performance
        /*dataOutput.writeInt(neighbors.size());
        for (I n:(Set<I>) neighbors) {
            n.write(dataOutput);
        }*/
        /*for(I n: map.keySet()) {
            n.write(dataOutput);
            dataOutput.writeBoolean((boolean) map.get(n));
        }*/
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        senderId = (I)new LongWritable(dataInput.readLong());
        iteration = dataInput.readLong();
        //int sz = dataInput.readInt();
        /*Set<I> neighbors = new HashSet<I>(sz);
        for(int i=0; i<sz; i++) {
            neighbors.add((I)new LongWritable(dataInput.readLong()));
        }
        setNeighbors(neighbors);*/
        this.initialize_map();
        /*for(int i=0; i<sz; i++) {
            I n = ((I)new LongWritable(dataInput.readLong()));
            boolean b = dataInput.readBoolean();
            if(b) {
                this.setMessageReadByNeighbor(n);
            } else {
                this.markUnreadByNeighbor(n);
            }
        }*/
    }
}
