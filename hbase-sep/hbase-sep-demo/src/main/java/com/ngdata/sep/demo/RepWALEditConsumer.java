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
package com.ngdata.sep.demo;

import java.io.IOException;
import java.util.List;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.*;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A Consumer for Splice Replication:
 * 1. Update the Region-LSN table
 * 2. replicate the edits
 */
public class RepWALEditConsumer {
    final private static TableName regionLSNTableName = TableName.valueOf("regionLSNTable");
    final private static byte[] regionLSNCFName = Bytes.toBytes("regionLSNCFName");
    final private static byte[] regionLSNRowName = Bytes.toBytes("regionLSNRowName");


    public static void main(String[] args) throws Exception {
        // Create a configuratin and set replication to true
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        // Create the zoo keeper connection
        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);

        // Create SepModelImpl and set up the zk nodes
        SepModel sepModel = new SepModelImpl(zk, conf);


        final String subscriptionName = "replication";

        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }

        createRegionLSNTable(conf);

        // Create consumer that simulates a Region Server and receives replicated WALEdit
        RepConsumer repConsumer = new RepConsumer(regionLSNTableName,
                regionLSNCFName, regionLSNRowName, subscriptionName, 0,
                "localhost", zk, conf, true);

        repConsumer.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    public static void createRegionLSNTable(Configuration hbaseConf) throws IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        if(!admin.tableExists(regionLSNTableName)){
            HTableDescriptor tableDescriptor = new HTableDescriptor(regionLSNTableName);
            HColumnDescriptor infoCf = new HColumnDescriptor(regionLSNCFName);
            tableDescriptor.addFamily(infoCf);
            admin.createTable(tableDescriptor);
        }
        assert(admin.tableExists(regionLSNTableName));
        admin.close();
    }
}