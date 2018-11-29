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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/*
 * TimestampIncre is used to bump up the timestamp at the Slave Cluster
 */
public class TimestampIncre {

    // table at the slave cluster, stores each region's LSN and the timestamp
    // when the snapshot is taken.
    final static TableName masterLSNTS = TableName.valueOf("masterLSNTS");
    final static byte[] theCF = Bytes.toBytes("CF1");
    // the one column that is used to store the timestamp
    final static byte[] tsCol = Bytes.toBytes("ts");
    // only using one row for masterLSNTS table
    final static byte[] theRow = Bytes.toBytes("theRow");
    // Table at Slave cluster, storing the largest LSN that have been applied to each

    // replicated regions and their corresponding largest LSN
    final private static TableName regionLSNTableName = TableName.valueOf("regionLSNTable");
    final private static byte[] regionLSNCFName = Bytes.toBytes("regionLSNCFName");
    // there is only one row in regionLSNTableName
    final private static byte[] regionLSNRowName = Bytes.toBytes("regionLSNRowName");


    public static void main(String[] args) throws Exception {
        Connection conn = null;
        try {
            // make connection to the local cluster's HBase
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            // check if both table regionLSNTable and masterLSNTS exit
            System.out.println("started TimestampIncre");
            while (!(admin.tableExists(masterLSNTS) && admin.tableExists(regionLSNTableName))) {
                // sleep for 1 seconds and retry.
                try {
                    Thread.sleep(5000);
                    System.out.println("waiting for table to be created");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            admin.close();

            System.out.println("the two tables come online");

            Long currTs = null;
            Long nextTs = null;
            Boolean increTs = false;
            Map<String, Long> lsnTsMap = new HashMap<String, Long>();
            nextTs = fillMap(conn, lsnTsMap);
            System.out.println("nextTs is " + nextTs.toString());

            while (true) {
                if(nextTs == null){
                    System.out.println("cannot find nextTs from lsnTsMap. Exit with error");
                    System.exit(1);
                }
                if (increTs) {
                    // bumps up the current Timestamp and reset lsnTsMap
                    Long prevTs = currTs;
                    currTs = nextTs;
//                    System.out.printf("update timestamp from %d to %d\n", prevTs, currTs);
                    lsnTsMap.clear();
                    nextTs = fillMap(conn, lsnTsMap);
                }

                increTs = true;

                Table regionLSNTable = conn.getTable(regionLSNTableName);
                Get getRegionLSN = new Get(regionLSNRowName);
                Result regionLSNRow = regionLSNTable.get(getRegionLSN);
                CellScanner regionLSNScanner = regionLSNRow.cellScanner();
                int regionCount = 0;
                while (regionLSNScanner.advance()) {
                    Cell cell = regionLSNScanner.current();
                    String region = Bytes.toString(cell.getQualifier());
                    Long seqNum = Bytes.toLong(cell.getValue());
                    if (!lsnTsMap.containsKey(region)) {
                        // this case should not happen, it means that
                        // a region's LSN has been written but the actual data
                        // has not been replicated yet
                        System.out.println("lsnTsMap does not contain a region");
                        increTs = false;
                        break;
                    } else {
                        Long targetSeqNum = lsnTsMap.get(region);
                        System.out.println("target SeqNum " + targetSeqNum + " slave SeqNum: " + seqNum.toString());
                        if (targetSeqNum > seqNum) {
                            increTs = false;
                            break;
                        }
                        regionCount++;
                    }
                }

                // currently does not check whether the number of regions match or not
                // since not all the tables are currently being replicated
//            if(regionCount < lsnTsMap.size()){
//                increTs = false;
//            }
                regionLSNTable.close();
                System.out.println(String.format("after iteration nextTs is %d, currTs is %d", nextTs, currTs));
            }
        }
        catch(Exception e){
            System.out.println("Exit due to experiencing Exception" + e.toString());
        }
        finally{
            if(conn != null){
                conn.close();
            }
        }
    }

    /*
     * fill the lsnTsMap by looking at the regionLSNTable
     */
    static Long fillMap(Connection conn, Map<String, Long> map) throws Exception{

        Table masterLSNTSTable = conn.getTable(masterLSNTS);
        Get getMasterLSNTS = new Get(theRow);
        Result masterLSNTSRow = masterLSNTSTable.get(getMasterLSNTS);
        CellScanner scanner = masterLSNTSRow.cellScanner();
        Long nextTs = null;
        while (scanner.advance()){
            // iterate through all the columns in the row
            Cell cell = scanner.current();
            byte[] colName = cell.getQualifier();
            System.out.println(Bytes.toString(colName));
            if(Arrays.equals(colName, tsCol)){
                nextTs = Bytes.toLong(cell.getValue());
            }
            else{
                String region = Bytes.toString(colName);
                Long seqNum = Bytes.toLong(cell.getValue());
                map.put(region, seqNum);
            }
        }
        masterLSNTSTable.close();
        return nextTs;
    }
}
