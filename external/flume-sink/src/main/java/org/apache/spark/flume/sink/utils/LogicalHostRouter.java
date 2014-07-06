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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class LogicalHostRouter {
    private static final Logger LOG = LoggerFactory.getLogger(LogicalHostRouter.class);
    private final Conf conf;
    private List<LogicalHostRouterListener> listeners = new
            CopyOnWriteArrayList<LogicalHostRouterListener>();
    private ZkProxy zkProxy = null;
    private volatile boolean watchChildren = false;

    public LogicalHostRouter(Conf conf) {
        this.conf = conf;
        this.zkProxy = ZkProxy.get(new ZkProxy.Conf(conf.zkAddress, conf.zkRetryTimes,
                conf.zkRetryIntervalInMs));
    }

    public void start() throws IOException {
        try {
            LOG.info("begin to start logical host router");
            zkProxy.start();
        } catch (Exception e) {
            LOG.error("failed to start zkProxy:" + e.getMessage());
            throw new IOException("failed to start logical host router:" + e.getMessage());
        }
    }

    public void stop() {
        LOG.info("stop logical host router");
        zkProxy.stop();
        zkProxy = null;
    }

    // register a physical host to logical host
    public void registerPhysicalHost(String logicalHost, PhysicalHost hostInfo) throws IOException {
        String zkNodePath = getZkNodePath(logicalHost, hostInfo);
        try {
            zkProxy.create(zkNodePath, new byte[0], ZkProxy.ZkNodeMode.EPHEMERAL, true);
        } catch (Exception e) {
            throw new IOException("failed to register to " + zkNodePath, e);
        }
    }

    // remove a physical host from logical host
    public void unregisterPhysicalHost(String logicalHost, PhysicalHost hostInfo) throws
            IOException {
        String zkNodePath = getZkNodePath(logicalHost, hostInfo);
        try {
            zkProxy.delete(zkNodePath);
        } catch (Exception e) {
            throw new IOException("failed to unregister " + zkNodePath, e);
        }
    }

    /*
     * register a listener to watch physical hosts changes.
     *
     * Notice: Currently, LogicalHostAdded event is not supported
     */
    public synchronized void registerListener(LogicalHostRouterListener listener) throws IOException {
        if (!watchChildren) {
            try {
                // TODO registerEventListener on zkPath to watch logical host changes
                List<String> logicalHosts = zkProxy.getChildren(conf.zkPath);
                for (String logicalHost : logicalHosts) {
                    PhysicalHostUpdateEventListener zkListener = new
                            PhysicalHostUpdateEventListener();
                    String zkPath = getZkNodePath(logicalHost);
                    zkProxy.registerEventListener(zkPath, zkListener);
                }
            } catch (Exception e) {
                LOG.error("failed to registerListener:" + e.getMessage());
                throw new IOException(e);
            }
            watchChildren = true;
        }
        listeners.add(listener);
    }

    public void unregisterListener(LogicalHostRouterListener listener) {
        listeners.remove(listener);
    }

    public List<String> getLogicalHosts() throws IOException {
        String zkNodePath = conf.zkPath;
        try {
            return zkProxy.getChildren(zkNodePath);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public List<PhysicalHost> getPhysicalHosts(String logicalHost) throws IOException {
        String zkNodePath = getZkNodePath(logicalHost);
        List<PhysicalHost> results = new ArrayList<PhysicalHost>();
        try {
            List<String> children = zkProxy.getChildren(zkNodePath);
            for (String child : children) {
                results.add(getPhysicalHost(child));
            }
        } catch (Exception e) {
            throw new IOException("failed to get physical hosts from " + zkNodePath, e);
        }
        return results;
    }

    private void processPhysicalHostAddedEvent(String logicalHost, PhysicalHost physicalHost) {
        for (LogicalHostRouterListener listener : listeners) {
            listener.physicalHostAdded(logicalHost, physicalHost);
        }
    }

    private void processPhysicalHostRemovedEvent(String logicalHost, PhysicalHost physicalHost) {
        for (LogicalHostRouterListener listener : listeners) {
            listener.physicalHostRemoved(logicalHost, physicalHost);
        }
    }

    private PhysicalHost getPhysicalHost(String zkNodeName) {
        String[] addr = zkNodeName.split(":");
        Preconditions.checkState(addr.length == 2, addr + " is not in host:port format");
        PhysicalHost host = new PhysicalHost(addr[0], Integer.parseInt(addr[1]));
        return host;
    }

    private String getZkNodeName(PhysicalHost host) {
        return host.ip + ":" + host.port;
    }

    private String getZkNodePath(String logicalHost) {
        return conf.zkPath + "/" + logicalHost;
    }

    private String getZkNodePath(String logicalHost, PhysicalHost hostInfo) {
        return getZkNodePath(logicalHost) + "/" + getZkNodeName(hostInfo);
    }

    public static interface LogicalHostRouterListener {
        /*
         * one physical host was added to logicalHost
         */
        void physicalHostAdded(final String logicalHost, final PhysicalHost hostInfo);

        void physicalHostRemoved(final String logicalHost, final PhysicalHost hostInfo);
    }

    public static class PhysicalHost {
        private String ip;
        private int port;

        public PhysicalHost(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }

    public static class Conf {
        private String zkAddress;
        private String zkPath;
        private int zkRetryIntervalInMs = 1000;
        private int zkRetryTimes = 3;

        public Conf setZkAddress(String zkAddress) {
            this.zkAddress = zkAddress;
            return this;
        }

        public Conf setZkPath(String zkPath) {
            this.zkPath = zkPath;
            return this;
        }

        public Conf setRetryTimes(int retryTimes) {
            this.zkRetryTimes = retryTimes;
            return this;
        }

        public Conf setRetryInterval(int retryIntervalInMs) {
            this.zkRetryIntervalInMs = retryIntervalInMs;
            return this;
        }

        public static Conf fromRouterPath(String routerPath) {
            int index = routerPath.indexOf("/");
            return new Conf().setZkAddress(routerPath.substring(0,
                    index)).setZkPath(routerPath.substring(index));
        }
    }

    private class PhysicalHostUpdateEventListener implements ZkProxy.EventListener {
        private static final String CHARSET = "UTF-8";

        private Info getInfo(String zkNodePath, String routerPath) {
            int preIndex = zkNodePath.indexOf(conf.zkPath);
            if (preIndex < 0) {
                return null;
            }
            String[] parts = zkNodePath.substring(preIndex + 1).split("/");
            if (parts.length == 2) {
                Info info = new Info();
                String[] hostInfo = parts[1].split(":");
                if (hostInfo.length == 2) {
                    try {
                        info.logicalHost = parts[0];
                        info.physicalHost = new PhysicalHost(hostInfo[0],
                                Integer.parseInt(hostInfo[1]));
                        return info;
                    } catch (NumberFormatException e) {
                        LOG.error(e.getMessage());
                    }
                }
            }
            return null;
        }

        @Override
        public void process(CuratorFramework client, WatchedEvent watchedEvent,
                            CuratorEvent curatorEvent) {
            String eventPath = watchedEvent.getPath();
            Info info = getInfo(eventPath, conf.zkPath);
            if (info == null) {
                return;
            }
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                processPhysicalHostRemovedEvent(info.logicalHost, info.physicalHost);
            } else if (watchedEvent.getType() == Watcher.Event.EventType.NodeCreated) {
                processPhysicalHostAddedEvent(info.logicalHost, info.physicalHost);
            }
        }

        private class Info {
            String logicalHost;
            PhysicalHost physicalHost;
        }
    }

}
