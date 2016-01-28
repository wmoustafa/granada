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

package org.apache.giraph.comm.netty;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.messages.SimpleMessageStore;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

/**
 * Netty worker server that implement {@link WorkerServer} and contains
 * the actual {@link ServerData}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerServer<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements WorkerServer<I, V, E> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(NettyWorkerServer.class);
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> service;
  /** Netty server that does that actual I/O */
  private final NettyServer nettyServer;
  /** Server data storage */
  private final ServerData<I, V, E> serverData;
  /** Mapper context */
  private final Mapper<?, ?, ?, ?>.Context context;

  /**
   * Constructor to start the server.
   *
   * @param conf Configuration
   * @param service Service to get partition mappings
   * @param context Mapper context
   * @param exceptionHandler handle uncaught exceptions
   */
  public NettyWorkerServer(ImmutableClassesGiraphConfiguration<I, V, E> conf,
      CentralizedServiceWorker<I, V, E> service,
      Mapper<?, ?, ?, ?>.Context context,
      Thread.UncaughtExceptionHandler exceptionHandler) {
    this.conf = conf;
    this.service = service;
    this.context = context;

    serverData =
        new ServerData<I, V, E>(service, conf, createMessageStoreFactory(),
            context);

    nettyServer = new NettyServer(conf,
        new WorkerRequestServerHandler.Factory<I, V, E>(serverData),
        service.getWorkerInfo(), context, exceptionHandler);
    nettyServer.start();
  }

  /**
   * Decide which message store should be used for current application,
   * and create the factory for that store
   *
   * @return Message store factory
   */
  private MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
  createMessageStoreFactory() {
    Class<? extends MessageStoreFactory> messageStoreFactoryClass =
        MESSAGE_STORE_FACTORY_CLASS.get(conf);

    MessageStoreFactory messageStoreFactoryInstance =
        ReflectionUtils.newInstance(messageStoreFactoryClass);
    messageStoreFactoryInstance.initialize(service, conf);

    return messageStoreFactoryInstance;
  }

  @Override
  public InetSocketAddress getMyAddress() {
    return nettyServer.getMyAddress();
  }


    @Override
    public void prepareSuperstep() {

        if(serverData.getIncomingMessageStore() == null)
            serverData.prepareSuperstep(); // updates the current message-store
        else {
            Object2ObjectOpenHashMap[] map = ((SimpleMessageStore) serverData.getIncomingMessageStore()).getMap();

            for(int pId = 0; pId< map.length; pId++) {
                if(map[pId] != null) {
                    Object2ObjectOpenHashMap vertex_map =  map[pId];
                    for (I vertex_id : (Set<I>)vertex_map.keySet()) {
                        DataInputOutput messages = (DataInputOutput) vertex_map.get(vertex_id);
                        Object2IntOpenHashMap pos_map = ((SimpleMessageStore) serverData.getIncomingMessageStore()).get_position_map(pId);
                        if (!pos_map.containsKey(vertex_id)) {
                            pos_map.put(vertex_id, 0);
                            //TODO put in comments for while loop
                            //continue;
                        }
                        int current_pos = ((SimpleMessageStore) serverData.getIncomingMessageStore()).getCurrentPos(pId, vertex_id);
                        try {
                            int elements = (((UnsafeByteArrayOutputStream) messages.getDataOutput()).getPos() - current_pos);

                            if (elements > 0) {
                                ((UnsafeByteArrayOutputStream) messages.getDataOutput()).vicky_write(
                                        ((UnsafeByteArrayOutputStream) messages.getDataOutput()).getByteArray(),
                                        current_pos, elements);
                                //TODO set flag needed for while loop
                                ((SimpleMessageStore) serverData.getIncomingMessageStore()).get_has_messages_map(pId).put(vertex_id, true);
                            }
                            else if (elements == 0) { // read all messages
                                ((UnsafeByteArrayOutputStream) messages.getDataOutput()).reset();
                            }
                            pos_map.put(vertex_id, 0);
                        } catch (IOException e) {
                            LOG.error(e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }



  @Override
  public ServerData<I, V, E> getServerData() {
    return serverData;
  }

  @Override
  public void close() {
    nettyServer.stop();
  }
}