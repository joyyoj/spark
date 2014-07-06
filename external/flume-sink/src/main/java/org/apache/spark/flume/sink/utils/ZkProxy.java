/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.flume.sink.utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkProxy {
    private static final Logger logger = LoggerFactory.getLogger(ZkProxy.class);
    private static final byte[] BYTE_NULL = new byte[0];
    private final ProxyWatcher proxyWatcher = new ProxyWatcher();
    private final CuratorFramework curatorClient;
    private final AtomicReference<State> state;
    private final ConcurrentHashMap<String, EventListener> listeners = new ConcurrentHashMap<String, EventListener>();
//    private static LoadingCache<Conf, ZkProxy> zkProxys = CacheBuilder.newBuilder()
//            .weakValues().removalListener(new RemovalListener<Conf, ZkProxy>() {
//                @Override
//                public void onRemoval(RemovalNotification<Conf, ZkProxy> objectObjectRemovalNotification) {
//                    objectObjectRemovalNotification.getValue().stop();
//                }
//            }).build(new CacheLoader<Conf, ZkProxy>() {
//                @Override
//                public ZkProxy load(Conf zkConf) throws Exception {
//                    return new ZkProxy(zkConf);
//                }
//            });

    public static class Conf {
        public Conf(String zkAddress) {
            this(zkAddress, 3, 1000);
        }

        public Conf(String zkAddress, int retryTimes, int retryIntervalInMs) {
            this.address = zkAddress;
            this.retryTimes = retryTimes;
            this.retryIntervalInMs = retryIntervalInMs;
        }

        private String address;
        private int retryTimes;
        private int retryIntervalInMs;
    }

    public enum ZkNodeMode {
        PERSISTENT,
        EPHEMERAL,
    }

    public enum State {
        INIT,      // start() has not yet been called
        STARTED,   // start() has been called
        STOPPED    // stop() has been called
    }

    public interface EventListener {
        // TODO abstract ZkEvent instead depends on CuratorClient directly
        void process(CuratorFramework client, final WatchedEvent event, CuratorEvent curatorEvent);
    }

    private class ProxyWatcher implements Watcher {
        @Override
        public void process(final WatchedEvent event) {
            // We will receive None type event when connection is breaking.
            if (event.getType() == Watcher.Event.EventType.None) {
                logger.warn("ZkProxy Ingore the event: {}", event);
                return;
            }
            try {
                curatorClient.getData().usingWatcher(this)
                        .inBackground(
                                new BackgroundCallback() {
                                    @Override
                                    public void processResult(CuratorFramework client, CuratorEvent curatorEvent) {
                                        String zkPath = event.getPath();
                                        logger.debug("Received event from path: {} event: {}", zkPath, event.getType());
                                        dispatchEvent(client, event, curatorEvent);
                                    }
                                }
                        ).forPath(event.getPath());
            } catch (Exception e) {
                logger.warn("processZkEvent {} getData() Exception:{}", event, e);
            }
        }
    }

    private ZkProxy(Conf conf) {
        this.curatorClient = CuratorFrameworkFactory.newClient(conf.address,
                new RetryNTimes(conf.retryTimes, conf.retryIntervalInMs));
        this.state = new AtomicReference<State>(State.INIT);
    }

    public static ZkProxy get(Conf conf) {
       return new ZkProxy(conf);
    }

    public State getState() {
        return this.state.get();
    }

    public void start() {
        logger.info("ZkProxy starting ...");
        if (state.compareAndSet(State.INIT, State.STARTED)) {
            curatorClient.start();
            logger.info("ZkProxy started success!");
        } else {
            logger.warn("ZkProxy cannot be started, current state is " + state.get());
        }
    }

    public void stop() {
        logger.info("ZkProxy Stoping...");
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            curatorClient.close();
            logger.info("ZkProxy is Stoped!");
        } else {
            logger.warn("ZkProxy is not started when call stop");
        }
    }

    public void create(String zkNodePath, byte[] value, ZkNodeMode mode,
                       boolean creatingParentsIfNeeded) throws Exception {
        if (curatorClient.checkExists().forPath(zkNodePath) != null) {
            logger.error(zkNodePath + " was already exists!");
            throw new Exception(zkNodePath + " was already exists!");
        }

        // If value = null, Curator will set zkNode a default value: IP addr.
        // But it's not expected, so we set value to byte[0] if value is null
        byte[] bytes = value == null ? BYTE_NULL : value;
        if (creatingParentsIfNeeded) {
            curatorClient.create().creatingParentsIfNeeded().withMode(getZkCreateMode(mode)).forPath(zkNodePath, bytes);
        } else {
            curatorClient.create().withMode(getZkCreateMode(mode)).forPath(zkNodePath, bytes);
        }
    }

    public boolean checkExists(String zkNodePath) throws Exception {
        return curatorClient.checkExists().forPath(zkNodePath) != null;
    }

    // Delete a node and return its value
    // Notice: The deleted zkNodePath will not be listened any more.
    // if you want listen zkNodePath after deleted, you need call registerEventListener() again.
    public byte[] delete(String zkNodePath) throws Exception {
        byte[] value = BYTE_NULL;
        if (curatorClient.checkExists().forPath(zkNodePath) != null) {
            try {
                value = get(zkNodePath);
            } catch (Exception e) {
                logger.warn("get({}) in delete() Exception {}", zkNodePath, e);
            }
            curatorClient.delete().guaranteed().forPath(zkNodePath);
        } else {
            logger.warn("Failed to remove {}, path does not exist.", zkNodePath);
        }
        return value;
    }

    public byte[] get(String zkNodePath) throws Exception {
        return curatorClient.getData().forPath(zkNodePath);
    }

    public List<String> getChildren(String zkNodePath) throws Exception {
        return curatorClient.getChildren().forPath(zkNodePath);
    }

    public void set(String zkNodePath, byte[] value) throws Exception {
        curatorClient.setData().forPath(zkNodePath, value);
    }

    // Add a listener to a zkNode, which will Keep watching the change of the zkNode.
    // When the zkNodePath is deleted, the listener will be invalid.
    public void registerEventListener(String zkNodePath, EventListener eventListener) {
        try {
            curatorClient.checkExists().usingWatcher(proxyWatcher).inBackground().forPath(zkNodePath);
        } catch (Exception e) {
            logger.error("checkExists {} Exception.{}" + zkNodePath, e);
        }
    }

    public void unregisterEventListener(String zkNodePath) {
        this.listeners.remove(zkNodePath);
    }

    private CreateMode getZkCreateMode(ZkNodeMode mode) {
        return mode == ZkNodeMode.EPHEMERAL ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
    }

    private void dispatchEvent(CuratorFramework curatorClient, WatchedEvent watchedEvent, CuratorEvent curatorEvent) {
        String zkPath = watchedEvent.getPath();
        Preconditions.checkState(zkPath.equals(curatorEvent.getPath()));
        EventListener listener = listeners.get(zkPath);
        if (listener == null) {
            logger.info("The event at path [{}] not have a listener", zkPath);
            return;
        }
        listener.process(curatorClient, watchedEvent, curatorEvent);
        switch (watchedEvent.getType()) {
            case NodeDeleted:
                // zkNode be deleted, delete local listener at the sametime,
                // to avoid listenerMap grow unlimited.
                unregisterEventListener(zkPath);
                break;
        }
    }
}

