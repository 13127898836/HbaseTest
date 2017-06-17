package hbase;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Hadoop 2.7.3
 * Hbase 1.2.6
 * Created by chenchenghao on 2017/6/16.
 */
public class HbaseTest1 {
    public static  final  String TABLE_NAME="stu3";
    private static final String FAMILY_NAME = "f1";
    private static final String ROW_KEY1 = "r1";
    private static final String ROW_KEY2 = "r2";
    private static final String COLUMN_AGE = "age";
    private static final String COLUMN_NAME  = "name";

    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;//Admin 对Hbase进行ddl的核心类
    static {
        //构造能访问Hbase的Configuration对象
        conf = HBaseConfiguration.create();
        // conf.set("hbase.rootdir","file:///D:/HBase/hbase-1.2.6/root");
        conf.set("hbase.zookeeper.quorum","127.0.0.1:2181");
        try {
            connection =   ConnectionFactory.createConnection(conf);

            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  void main(String args[]){
        try {
            if(!admin.tableExists(TableName.valueOf(TABLE_NAME))){
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            hTableDescriptor.addFamily(new HColumnDescriptor(FAMILY_NAME));
            admin.createTable(hTableDescriptor);
            }else{
                System.out.println("Table exists");
            }
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            List<Put> datalit = new ArrayList<>();
            Put put1 = new Put(ROW_KEY1.getBytes());
            Put put2 = new Put(ROW_KEY2.getBytes());

            put1.addColumn(FAMILY_NAME.getBytes(),COLUMN_AGE.getBytes(),"24".getBytes());
            put1.addColumn(FAMILY_NAME.getBytes(),COLUMN_NAME.getBytes(),"zhangsan".getBytes());

            put2.addColumn(FAMILY_NAME.getBytes(),COLUMN_AGE.getBytes(),"25".getBytes());
            put2.addColumn(FAMILY_NAME.getBytes(),COLUMN_NAME.getBytes(),"lisi".getBytes());
            datalit.add(put1);
            datalit.add(put2);
            table.put(datalit);

            Get get1 = new Get(ROW_KEY1.getBytes());
            Get get2 = new Get(ROW_KEY2.getBytes());
            List<Get> getList = new ArrayList<>();
            String name="";
            String age="";
            Result result=null;
            result = table.get(get1);
           name= new String(result.getValue(FAMILY_NAME.getBytes(),COLUMN_NAME.getBytes()));
            age = new String(result.getValue(FAMILY_NAME.getBytes(),COLUMN_AGE.getBytes()));
            System.out.println("result name:"+name+" age:"+age);

            Scan scan = new Scan();
            scan.setStartRow(ROW_KEY1.getBytes());
            scan.setStopRow(ROW_KEY2.getBytes());
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result1:resultScanner){
                 name = new String(result1.getValue(FAMILY_NAME.getBytes(),COLUMN_NAME.getBytes()));
                 age = new String(result1.getValue(FAMILY_NAME.getBytes(),COLUMN_AGE.getBytes()));
                System.out.println("result1 name:"+name+" age:"+age);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
