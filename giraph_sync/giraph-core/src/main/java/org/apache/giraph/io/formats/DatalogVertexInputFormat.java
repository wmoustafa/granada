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

package org.apache.giraph.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Vicky Papavasileiou on 11/30/15.
 */
public class DatalogVertexInputFormat extends TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(DatalogVertexInputFormat.class);



    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException {

        return new DatalogVertexReader();
    }


    /**
     * Vertex reader associated with
     * {@link DatalogVertexInputFormat}.
     */
    public class DatalogVertexReader  extends TextVertexReader {
        /** Cached vertex id for the current line */
        private LongWritable id;
        /** Cached vertex value for the current line */
        private DoubleWritable value;

        private Vertex<LongWritable, DoubleWritable, FloatWritable> vertex;

        private  List<Edge<LongWritable, FloatWritable>> edges;

        @Override
        public final Vertex<LongWritable, DoubleWritable, FloatWritable>  getCurrentVertex()
                throws IOException, InterruptedException {

            Text line = getRecordReader().getCurrentValue();

            String[] tokens = SEPARATOR.split(line.toString());

            id = new LongWritable(Long.parseLong(tokens[1]));
            value = new DoubleWritable(Double.parseDouble(tokens[2]));
            LOG.info("Read vertex with id " + id);
            vertex = getConf().createVertex();
            edges = Lists.newArrayList();

            int counter = 3;
            while(counter < tokens.length) {
                LOG.info("Read edge to neighbor " + tokens[counter+2]);
                edges.add(EdgeFactory.create(new LongWritable(Long.parseLong(tokens[counter+2])),
                        new FloatWritable(Float.parseFloat(tokens[counter+3]))));
                counter+=4;
            }

            vertex.initialize(id,value,edges);

            return vertex;

        }

        @Override
        public final boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }


    }
}
