package HBase;
/**
 * Created by tosnos on 02/11/16.
 */

import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Date;
import java.text.SimpleDateFormat;

public class HBaseSocialNetwork {

    public static Writable toWritable(ArrayList<String> list) {
        Writable[] content = new Writable[list.size()];
        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }
        return new ArrayWritable(Text.class, content);
    }

    public static ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);
        for (Writable wrt : writables) {
            list.add(((Text)wrt).toString());
        }
        return list;
    }

    public static void main (String args[]) throws IOException {

        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "vbrunelHBaseTable");
        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        boolean friendExit = false;

        while(!exit){

            System.out.println("Choose any option :");
            System.out.println("1. Add a new personn");
            System.out.println("2. Exit");

            String option = scanner.next();
            String bestFriend;

            if (option.equals("1")) {
                ArrayList<String> friends = new ArrayList<String>();
                System.out.println("First Name :");
                String id = scanner.next();
                System.out.println("Family Name :");
                String familyName = scanner.next();
                do {
                    System.out.println("BFF :");
                    bestFriend = scanner.next();
                } while (bestFriend.equals(""));
                System.out.println("Birthday :");
                String birthDate = scanner.next();
                System.out.println("Address :");
                String address = scanner.next();
                System.out.println("Phone Number :");
                String phoneNumber = scanner.next();
                System.out.println("Description :");
                String description = scanner.next();
                while(!friendExit){
                    System.out.println("1. Add New Friend");
                    System.out.println("2. Continue");
                    String optionFriend = scanner.next();

                    if(optionFriend.equals("1")){
                        System.out.println("Friend Name :");
                        String newFriend = scanner.next();
                        friends.add(newFriend);
                    }

                    if(optionFriend.equals("2")){
                        friendExit=true;
                    }

                    else{
                        System.out.println("Type 1 or 2");
                    }


                }

                /***
                 *
                 */
                Put p = new Put(Bytes.toBytes(id));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("familyName"), Bytes.toBytes(familyName));
                p.add(Bytes.toBytes("friends"), Bytes.toBytes("BFF"), Bytes.toBytes(bestFriend));
                p.add(Bytes.toBytes("friends"), Bytes.toBytes("friendList"), WritableUtils.toByteArray(toWritable(friends)));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes(birthDate));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("phoneNumber"), Bytes.toBytes(phoneNumber));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(address));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("description"), Bytes.toBytes(description));
                table.put(p);

                System.out.println("******** PROFILE ADDED ********");

            } else if (option.equals("2")) {
                exit = true;

            } else {
                System.out.println("Option not available");

            }


        }

    }

}

