package org.example;

import com.github.mfathi91.time.PersianDate;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;
//import com.github.sbahmani.jalcal.util;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class BulkLoadDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        pool = new JedisPool("localhost", 6379);
        int result = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadDriver(), args);
        md = MessageDigest.getInstance("MD5");
    }

    public static enum COUNTER_TEST {FILE_FOUND, FILE_NOT_FOUND}
    public static JedisPool pool;
    public static MessageDigest md;
    public String tableName = "bmi";// name of the table to be inserted in hbase
    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final String OUTPUT_FORMAT = "%-20s:%s";
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
//
    private static boolean checkDate(String string) {
        // Converting the string to date
        // in the specified format
        try {
            PersianDate.parse(string);
        } catch (Exception e) {
            return false;
        }

        // Returning the converted date
        return true;
    }

    private static boolean checkRegexShaba(String string)
    {
        String sh_regex = "^(?:IR)(?=.{24}$)[0-9]*$";
        Pattern pattern = Pattern.compile(sh_regex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher("string").find();
    }

    private static boolean checkRegexBankCode(String string)
    {
        String bankCode_regex = "^[\"]+|\\s?[\"]+$";
        Pattern pattern = Pattern.compile(bankCode_regex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher("string").find();
    }

    private static boolean checkInt(String str) {
        try {
            Integer.parseInt(str);
        } catch (Exception e) {
            return false;
        }

        // Returning the converted date
        return true;
    }

    static String removeLeadingZeroes(String s) {

        s = s.replaceAll("^'+", "");
        s = s.replaceAll("^'+", "");
        return s.replaceAll("^\"+", "");
    }

    private static boolean check_Bank_Code(String str) {
        return checkRegexBankCode(removeTrailingZeroes(str));
    }

    private static boolean check_Account_No(String str) {
        return true;
    }

    private static boolean check_cust_No(String str) {
        return checkInt(removeTrailingZeroes(str));
    }

    private static boolean check_Account_Type(String str) {
        return true;
    }

    private static boolean check_Account_Type_Desc(String str) {
        return true;
    }

    private static boolean check_Account_Status(String str) {
        return true;
    }

    private static boolean check_Account_Status_Desc(String str) {
        return true;
    }

    private static boolean check_Open_Branch(String str) {
        return checkDate(removeTrailingZeroes(str));
    }

    private static boolean check_Open_Date(String str) {
        return checkDate(removeTrailingZeroes(str));
    }
    private static boolean check_Freeze_Date(String str) {
        return checkDate(removeTrailingZeroes(str));
    }
    private static boolean check_Close_Date(String str) {
        return checkDate(removeTrailingZeroes(str));
    }
    private static boolean check_Change_Date(String str) {
        return checkDate(removeTrailingZeroes(str));
    }
    private static boolean check_Sheba_No(String str) {
        return checkRegexShaba(removeTrailingZeroes(str));
    }
    private static boolean check_Row_Id(String str) {
        return true;
    }
    private static boolean check_hash (String str) {
        return true;
    }
    private static boolean check_ccno (String str) {
        return true;
    }

    static String removeTrailingZeroes(String s) {
        s.replaceAll("0+$", "");
        s = s.replaceAll("^'+", "");
        s = s.replaceAll("^'+", "");
        return s.replaceAll("^\"+", "");

    }

//    private static bool checkDate(String date)
//    {
//        Long time = 1520956290000l;
//        String j1 = JalCal.gregorianToJalali(new Date(time), false);
//        Date d1 = JalCal.JalaliToGregorianWithHourMinSec(j1);
//
//        Calendar expected4 = Calendar.getInstance(TimeZone.getDefault());
////        return new PersianDate.of(date);
////        JalaliCalendar jalaliCalendar = new JalaliCalendar(1395, 1, 28);
//    }

    @Override
    public int run(String[] args) throws Exception {

        //Configuration conf= this.getConf();
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "file:///");

        Job job = new Job(conf, "BulkLoad");
        job.setJarByClass(getClass());

        job.setMapperClass(bulkMapper.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        if (args.length > 1) {
            for (int i = 1; i < args.length; i++)
                MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, bulkMapper.class);
        }
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);   //for HBase table
        job.setNumReduceTasks(0);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    private static class bulkMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        //static class bulkMapper extends TableMapper<ImmutableBytesWritable, Put> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            for (int i = 0; i < val.length; i++) {
                val[i] = removeTrailingZeroes(val[i]);
            }
            byte[] CF = "account".getBytes();
            byte[] CF_F = "false_account".getBytes();

            // store the split values in the bytes format so that they can be added to the PUT object
            byte[] keyrow = toBytes(val[0] + val[1]);
            byte[] Bank_Code = toBytes(val[0]);
            byte[] Account_No = toBytes(val[1]);
            byte[] cust_No = toBytes(val[2]);
            byte[] Account_Type = toBytes(val[3]);
            byte[] Account_Type_Desc = toBytes(val[4]);
            byte[] Account_Status = toBytes(val[5]);
            byte[] Account_Status_Desc = toBytes(val[6]);
            byte[] Open_Branch = toBytes(val[7]);
            byte[] Open_Date = toBytes(val[8]);
            byte[] Freeze_Date = toBytes(val[9]);
            byte[] Close_Date = toBytes(val[10]);
            byte[] Change_Date = toBytes(val[11]);
            byte[] Sheba_No = toBytes(val[12]);
            byte[] Row_Id = toBytes(val[13]);
            byte[] filename = toBytes(val[14]);
            byte[] hash = toBytes(val[15]);
            byte[] ccno = toBytes(val[16]);

            Put put = new Put(keyrow);

            if (check_Bank_Code(val[0]) && check_Account_No(val[1]) && check_cust_No(val[2]) && check_Account_Type(val[3])
                    && check_Account_Type_Desc(val[4]) && check_Account_Status(val[5]) && check_Account_Status_Desc(val[6])
                    && check_Open_Branch(val[7]) && check_Open_Date(val[8]) && check_Freeze_Date(val[9]) &&
                    check_Close_Date(val[10]) && check_Change_Date(val[11]) && check_Sheba_No(val[12])) {

                put.addColumn(CF, "Bank_Code".getBytes(), Bank_Code);
                put.addColumn(CF, "Account_No".getBytes(), Account_No);
                put.addColumn(CF, "cust_No".getBytes(), cust_No);
                put.addColumn(CF, "Account_Type".getBytes(), Account_Type);
                put.addColumn(CF, "Account_Type_Desc".getBytes(), Account_Type_Desc);
                put.addColumn(CF, "Account_Status".getBytes(), Account_Status);
                put.addColumn(CF, "Account_Status_Desc".getBytes(), Account_Status_Desc);
                put.addColumn(CF, "Open_Branch".getBytes(), Open_Branch);
                put.addColumn(CF, "Open_Date".getBytes(), Open_Date);
                put.addColumn(CF, "Freeze_Date".getBytes(), Freeze_Date);
                put.addColumn(CF, "Close_Date".getBytes(), Close_Date);
                put.addColumn(CF, "Change_Date".getBytes(), Change_Date);
                put.addColumn(CF, "Sheba_No".getBytes(), Sheba_No);
                put.addColumn(CF, "Row_Id".getBytes(), Row_Id);
                put.addColumn(CF, "filename".getBytes(), filename);
                put.addColumn(CF, "hash".getBytes(), hash);
                put.addColumn(CF, "ccno".getBytes(), ccno);
            }
            else
            {
                put.addColumn(CF_F, "Bank_Code".getBytes(), Bank_Code);
                put.addColumn(CF_F, "Account_No".getBytes(), Account_No);
                put.addColumn(CF_F, "cust_No".getBytes(), cust_No);
                put.addColumn(CF_F, "Account_Type".getBytes(), Account_Type);
                put.addColumn(CF_F, "Account_Type_Desc".getBytes(), Account_Type_Desc);
                put.addColumn(CF_F, "Account_Status".getBytes(), Account_Status);
                put.addColumn(CF_F, "Account_Status_Desc".getBytes(), Account_Status_Desc);
                put.addColumn(CF_F, "Open_Branch".getBytes(), Open_Branch);
                put.addColumn(CF_F, "Open_Date".getBytes(), Open_Date);
                put.addColumn(CF_F, "Freeze_Date".getBytes(), Freeze_Date);
                put.addColumn(CF_F, "Close_Date".getBytes(), Close_Date);
                put.addColumn(CF_F, "Change_Date".getBytes(), Change_Date);
                put.addColumn(CF_F, "Sheba_No".getBytes(), Sheba_No);
                put.addColumn(CF_F, "Row_Id".getBytes(), Row_Id);
                put.addColumn(CF_F, "filename".getBytes(), filename);
                put.addColumn(CF_F, "hash".getBytes(), hash);
                put.addColumn(CF_F, "ccno".getBytes(), ccno);
            }

            byte[] md5 = md.digest(toBytes(value.toString()));
            String md5str = String.format(OUTPUT_FORMAT, bytesToHex(md5));
            System.out.println(keyrow);
            try (Jedis jedis = pool.getResource()) {
                if (!Objects.equals(md5str, jedis.get(val[0] + val[1]))) {
                    context.write(new ImmutableBytesWritable(keyrow), put);
                }else
                    jedis.set(val[0] + val[1], md5str);
            }

            context.getCounter(COUNTER_TEST.FILE_FOUND).increment(1);

        }
    }
}