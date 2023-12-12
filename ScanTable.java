package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class ScanTable{

    public static void main(String args[]) throws IOException{

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        // Instantiating HTable class
        Table table = connection.getTable(TableName.valueOf("bmi"));
                //new HTable(config, "emp");
        // Instantiating the Scan class
        Scan scan = new Scan();

        // Scanning the required columns
        scan.addColumn(Bytes.toBytes("account"), Bytes.toBytes("Bank_Code"));
        scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("Account_No"));

        // Getting the scan result
        ResultScanner scanner = table.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next())

            System.out.println("Found row : " + result);
        //closing the scanner
        scanner.close();
    }
}