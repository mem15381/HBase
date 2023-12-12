package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Main {
    public static void main(String[] args) {
        System.out.println("hbase test.........");
        System.out.println(getDistinctCol("bmi", "account", "Bank_Code"));
    }
    public static Set<String> getDistinctCol(String tableName, String colFamilyName, String colName) {
        Configuration config = HBaseConfiguration.create();

        // replace with your own configuration, such as specifying the zookeeper quorum
        config.set("hbase.zookeeper.quorum", "UDM-APP1,UDM-APP2,UDM-APP3");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // Connect to HBase cluster
        Connection connection;
        Set<String> set = new HashSet<String>();
        try {
            connection = ConnectionFactory.createConnection(config);

            // Get a reference to the table
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs;
            Result res;
            String s;

            //HTable table = new HTable(config, tableName);
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
            rs = table.getScanner(scan);
            while ((res = rs.next()) != null) {
                byte[] col = res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
                s = Bytes.toString(col);
                set.add(s);
            }
        } catch (IOException e) {
            System.out.println("Exception occured in retrieving data");
        }
        return set;
    }
}