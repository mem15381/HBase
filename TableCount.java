package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class TableCount {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("bmi");
        // replace with your own configuration, such as specifying the zookeeper quorum
        config.set("hbase.zookeeper.quorum", "UDM-APP1,UDM-APP2,UDM-APP3");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // Connect to HBase cluster
        Connection connection;
        int number = 0;
        try {
            connection = ConnectionFactory.createConnection(config);

            // Get a reference to the table
            Table table = connection.getTable(TableName.valueOf("bmi"));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);

            for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
                number++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("total count: " + number);
    }
}
