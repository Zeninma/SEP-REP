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
        // make connection to the local cluster's HBase
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        // check if both table regionLSNTable and masterLSNTS exit
        while(!admin.tableExists(masterLSNTS) || !admin.tableExists(regionLSNTableName)) {
            // sleep for 1 seconds and retry.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        Long currTs = null;
        Long nextTs = null;
        Boolean increTs = false;
        Map<String, Long> lsnTsMap = new HashMap<String, Long>();
        nextTs = fillMap(conn, lsnTsMap);

        while(true){
            if(increTs){
                // bumps up the current Timestamp and reset lsnTsMap
                currTs = nextTs;
                lsnTsMap.clear();
                nextTs = fillMap(conn, lsnTsMap);
            }

            increTs = true;

            Table regionLSNTable = conn.getTable(regionLSNTableName);
            Get getRegionLSN = new Get(regionLSNRowName);
            Result regionLSNRow = regionLSNTable.get(getRegionLSN);
            CellScanner regionLSNScanner = regionLSNRow.cellScanner();
            int regionCount = 0;
            while (regionLSNScanner.advance()){
                Cell cell = regionLSNScanner.current();
                String region = Bytes.toString(cell.getQualifierArray());
                Long seqNum = Bytes.toLong(cell.getValueArray());
                if(!lsnTsMap.containsKey(region)){
                    // this case should not happen, it means that
                    // a region's LSN has been written but the actual data
                    // has not been replicated yet
                    increTs = false;
                    break;
                }
                else{
                    Long targetSeqNum = lsnTsMap.get(region);
                    if(targetSeqNum >= seqNum){
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
            byte[] colName = cell.getQualifierArray();
            if(Arrays.equals(colName, tsCol)){
                nextTs = Bytes.toLong(cell.getValueArray());
            }
            else{
                String region = Bytes.toString(colName);
                Long seqNum = Bytes.toLong(cell.getValueArray());
                map.put(region, seqNum);
            }
        }
        masterLSNTSTable.close();
        return nextTs;
    }
}
