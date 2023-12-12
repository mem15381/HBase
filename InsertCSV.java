package org.example;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertCSV {
    public static void main(String[] args) {
        TableName table1 = TableName.valueOf("bmi");

        String family1 = "account";
        byte[] qualifier1 = Bytes.toBytes("Bank_Code");
        byte[] row1 = Bytes.toBytes("row1");

        Get g = new Get(row1);
        //Result r = table1.(g);
        //byte[] value = r.getValue(family1.getBytes(), qualifier1);

        Put p = new Put(row1);
        p.addImmutable(family1.getBytes(), qualifier1, Bytes.toBytes("cell_data"));
        //table1.put(p);
    }
}
