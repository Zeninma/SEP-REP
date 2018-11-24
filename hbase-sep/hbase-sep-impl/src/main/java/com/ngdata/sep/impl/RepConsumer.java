/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.util.concurrent.WaitPolicy;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * SepConsumer consumes the events for a certain SEP subscription and dispatches
 * them to an EventListener (optionally multi-threaded). Multiple SepConsumer's
 * can be started that take events from the same subscription: each consumer
 * will receive a subset of the events.
 *
 * <p>On a more technical level, SepConsumer is the remote process (the "fake
 * hbase regionserver") to which the regionservers in the hbase master cluster
 * connect to replicate log entries.</p>
 */
public class RepConsumer extends BaseHRegionServer {
    private final String subscriptionId;
    private long subscriptionTimestamp;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private RpcServer rpcServer;
    private ServerName serverName;
    private ZooKeeperWatcher zkWatcher;
    private SepMetrics sepMetrics;
    private String zkNodePath;
    boolean running = false;
    private Log log = LogFactory.getLog(getClass());
    // Added field for directly doing the replication
    private volatile Connection sharedHtableCon;
    private final Object sharedHtableConLock = new Object();
    // default names of the table and column familiy that store the
    // EncodedRegionName and LSN mapping
    private TableName regionLSNTableName;
    private byte[] regionLSNCFName;
    private byte[] regionLSNRowName;
    private boolean isTest = false;

    /**
     * @param subscriptionTimestamp timestamp of when the index subscription became active (or more accurately, not
     *                              inactive)
     * @param hostName              hostname to bind to
     */
    public RepConsumer(TableName newRegionLSNTableName, byte[] newRegionLSNCFName,
            byte[] newRegionLSNRowName, String subscriptionId, long subscriptionTimestamp,
                       String hostName, ZooKeeperItf zk, Configuration hbaseConf, boolean isTest) throws IOException{
        this.subscriptionId = SepModelImpl.toInternalSubscriptionName(subscriptionId);
        this.subscriptionTimestamp = subscriptionTimestamp;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.sepMetrics = new SepMetrics(subscriptionId);
        this.regionLSNTableName = newRegionLSNTableName;
        this.regionLSNCFName = newRegionLSNCFName;
        this.regionLSNRowName = newRegionLSNRowName;
        this.isTest = isTest;
        // Binding the SEP Consumer to a socket
        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
        if (initialIsa.getAddress() == null) {
            throw new IllegalArgumentException("Failed resolve of " + initialIsa);
        }
        String name = "regionserver/" + initialIsa.toString();

        // Construct new RPC server to expose the service
        this.rpcServer = new RpcServer(this, name, getServices(),
                /*HBaseRPCErrorHandler.class, OnlineRegions.class},*/
                initialIsa, // BindAddress is IP we got for this server.
                //hbaseConf.getInt("hbase.regionserver.handler.count", 10),
                //hbaseConf.getInt("hbase.regionserver.metahandler.count", 10),
                hbaseConf,
                new FifoRpcScheduler(hbaseConf, hbaseConf.getInt("hbase.regionserver.handler.count", 10)));
          /*
          new SimpleRpcScheduler(
            hbaseConf,
            hbaseConf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT),
            hbaseConf.getInt("hbase.regionserver.metahandler.count", 10),
            hbaseConf.getInt("hbase.regionserver.handler.count", 10),
            this,
            HConstants.QOS_THRESHOLD)
          );
          */
        this.serverName = ServerName.valueOf(hostName, rpcServer.getListenerAddress().getPort(), System.currentTimeMillis());
        this.zkWatcher = new ZooKeeperWatcher(hbaseConf, this.serverName.toString(), null);

        // login the zookeeper client principal (if using security)
//        ZKUtil.loginClient(hbaseConf, "hbase.zookeeper.client.keytab.file",
//                "hbase.zookeeper.client.kerberos.principal", hostName);

        // login the server principal (if using secure Hadoop)
//        User.login(hbaseConf, "hbase.regionserver.keytab.file",
//                "hbase.regionserver.kerberos.principal", hostName);
    }

    /*
     * Publish the service on Zookeeper for the Master replication cluster to use
     */
    public void start() throws IOException, InterruptedException, KeeperException {

        rpcServer.start();

        // Publish our existence in ZooKeeper
        zkNodePath = hbaseConf.get(SepModel.ZK_ROOT_NODE_CONF_KEY, SepModel.DEFAULT_ZK_ROOT_NODE)
                + "/" + subscriptionId + "/rs/" + serverName.getServerName();
        zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        this.running = true;
    }

    private List<RpcServer.BlockingServiceAndInterface> getServices() {
        List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<RpcServer.BlockingServiceAndInterface>(1);
        bssi.add(new RpcServer.BlockingServiceAndInterface(
                AdminProtos.AdminService.newReflectiveBlockingService(this),
                AdminProtos.AdminService.BlockingInterface.class));
        return bssi;
    }

    /*
     * Stop the service and remove the ZNode from Zookeeper.
     */
    public void stop() {
        Closer.close(zkWatcher);
        if (running) {
            running = false;
            Closer.close(rpcServer);
            try {
                // This ZK node will likely already be gone if the index has been removed
                // from ZK, but we'll try to remove it here to be sure
                zk.delete(zkNodePath, -1);
            } catch (Exception e) {
                log.debug("Exception while removing zookeeper node", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        sepMetrics.shutdown();
    }

    public boolean isRunning() {
        return running;
    }

    /*
     * Service exposed for replicating the WALEntries
     */
    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
                                                                   final AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
        List<AdminProtos.WALEntry> entries = request.getEntryList();
        CellScanner cells = ((PayloadCarryingRpcController)controller).cellScanner();

        // directly return on empty entry list
        if (entries.isEmpty()){
            return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
        }

        // Very simple optimization where we batch sequences of rows going
        // to the same table.
        try {
            long totalReplicated = 0;
            // Map of table => list of Rows, we only want to flushCommits once per
            // invocation of this method per table
            Map<TableName, List<Row>> rowMap =
                    new HashMap<>();
            // Map of EncodedRegionName => largest log sequence number has been seen
            Map<com.google.protobuf.ByteString, Long> regionLSNMap =
                    new HashMap<>();
            for (AdminProtos.WALEntry entry : entries) {
                // extract the EncodedRegionName and corresponding LSN from the given WALEntry
                WALProtos.WALKey walKey = entry.getKey();
                com.google.protobuf.ByteString encodedRegionName = walKey.getEncodedRegionName();
                long regionLSN = walKey.getLogSequenceNumber();
                addToRegionLSNMap(regionLSNMap, encodedRegionName, regionLSN);

                TableName table =
                        TableName.valueOf(entry.getKey().getTableName().toByteArray());
                Cell previousCell = null;
                Mutation m = null;
                int count = entry.getAssociatedCellCount();

                for (int i = 0; i < count; i++) {
                    // Throw index out of bounds if our cell count is off
                    if (!cells.advance()) {
                        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
                    }
                    Cell cell = cells.current();
                    if (isNewRowOrType(previousCell, cell)) {
                        // Create new mutation
                        m = CellUtil.isDelete(cell)?
                                new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()):
                                new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                        List<UUID> clusterIds = new ArrayList<UUID>();
                        m.setClusterIds(clusterIds);
                        addToRowMap(rowMap, table, m);
                    }
                    if (CellUtil.isDelete(cell)) {
                        ((Delete)m).addDeleteMarker(cell);
                    } else {
                        ((Put)m).add(cell);
                    }
                    previousCell = cell;
                }
                totalReplicated++;
            }

            // For each of the table schedule a replication task
            if(!isTest){
                for (Map.Entry<TableName, List<Row>> rowMapEntry : rowMap.entrySet()) {
                    batch(rowMapEntry.getKey(), rowMapEntry.getValue());
                }
            }

            // Update the EncodedRegion - LSN map in the Slave cluster
            Put updateRegionLSNPut = new Put(regionLSNRowName);

            for(Map.Entry<com.google.protobuf.ByteString, Long> regionLSNMapEntry:
            regionLSNMap.entrySet()){
                // Create a single row update containig
                byte[] regionCol = regionLSNMapEntry.getKey().toByteArray();
                updateRegionLSNPut.addColumn(regionLSNCFName, regionCol,
                        Bytes.toBytes(regionLSNMapEntry.getValue()));
            }
            // Apply the update
            applyPut(regionLSNTableName, updateRegionLSNPut);

//            int size = entries.size();
//            this.metrics.setAgeOfLastAppliedOp(entries.get(size - 1).getKey().getWriteTime());
//            this.metrics.applyBatch(size);
//            this.totalReplicatedEdits.addAndGet(totalReplicated);
            return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
        } catch (IOException ex) {
            throw new ServiceException(ex);
        }
    }

    @Override
    public Configuration getConfiguration() {
        return hbaseConf;
    }

    @Override
    public ServerName getServerName() {
        return serverName;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
        return zkWatcher;
    }


    /**
     * Do the changes and handle the pool
     * @param tableName table to insert into
     * @param rows list of actions
     * @throws IOException
     */
    protected void batch(TableName tableName, List<Row> rows) throws IOException {
        if (rows.isEmpty()) {
            return;
        }
        Table table = null;
        try {
            // See https://en.wikipedia.org/wiki/Double-checked_locking
            Connection connection = this.sharedHtableCon;
            if (connection == null) {
                synchronized (sharedHtableConLock) {
                    connection = this.sharedHtableCon;
                    if (connection == null) {
                        connection = this.sharedHtableCon = ConnectionFactory.createConnection(this.hbaseConf);
                    }
                }
            }
            table = connection.getTable(tableName);
            // replicate all the rows to the current cluster
            table.batch(rows);
        } catch (InterruptedException ix) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(ix);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * Do the changes and handle the pool
     * @param tableName table to insert into
     * @param rows list of actions
     * @throws IOException
     */
    protected void applyPut(TableName tableName, Put put) throws IOException {
        if (put == null || put.isEmpty()) {
            return;
        }
        Table table = null;
        try {
            // See https://en.wikipedia.org/wiki/Double-checked_locking
            Connection connection = this.sharedHtableCon;
            if (connection == null) {
                synchronized (sharedHtableConLock) {
                    connection = this.sharedHtableCon;
                    if (connection == null) {
                        connection = this.sharedHtableCon = ConnectionFactory.createConnection(this.hbaseConf);
                    }
                }
            }
            table = connection.getTable(tableName);
            // replicate all the rows to the current cluster
            table.put(put);
        } catch (IOException ioe) {
            throw (IOException) new IOException().initCause(ioe);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param previousCell
     * @param cell
     * @return True if we have crossed over onto a new row or type
     */
    private boolean isNewRowOrType(final Cell previousCell, final Cell cell) {
        return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte() ||
                !CellUtil.matchingRow(previousCell, cell);
    }

    /**
     * Add new cell into the corresponding List
     * @param map
     * @param key
     * @param val
     * @return
     */
    private <K, V> void addToRowMap(Map<K, List<V>> map, K key, V val){
        List<V> vals;
        if(map.containsKey(key)){
            vals = map.get(key);
        }
        else{
            vals = new ArrayList<>();
            map.put(key, vals);
        }
        vals.add(val);
        return;
    }

    /*
     * Helper function to update the regionLSNMap's value, if and only if
     * 1. the current encodedRegionName does not exist as a Key in the given regionLSNMap
     * or
     * 2. The given regionLSN is larger than the current value that is stored in the regionLSNMap
     */
    private void addToRegionLSNMap(Map<com.google.protobuf.ByteString, Long> regionLSNMap,
                                   com.google.protobuf.ByteString encodedRegionName,
                                   long regionLSN){
        if(regionLSNMap.containsKey(encodedRegionName)){
            long prevLSN = regionLSNMap.get(encodedRegionName);
            if(prevLSN >= regionLSN){
                return;
            }
        }

        regionLSNMap.put(encodedRegionName, regionLSN);
        return;
    }
}
