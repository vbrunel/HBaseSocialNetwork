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
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.text.SimpleDateFormat;

public class HBaseSocialNetwork {

    private static Configuration config;
    private static HTable table;

    /***
     * Function to convert Array into a Writable ArrayList
     * @param list
     * @return
     */

    public static Writable toWritable(ArrayList<String> list) {
        Writable[] content = new Writable[list.size()];
        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }
        return new ArrayWritable(Text.class, content);
    }

    /***
     * Function to convert ArrayWritable into an Arraylist
     * @param writable
     * @return
     */
    public static ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);
        for (Writable wrt : writables) {
            list.add(((Text)wrt).toString());
        }
        return list;
    }


    public static void main (String args[]) throws IOException {

        config = HBaseConfiguration.create();
        table = new HTable(config, "vbrunelHBaseTable");
        Scanner scanner = new Scanner(System.in);
        boolean exit = false; //first REPL
        boolean secondExit = false; //second REPL

        //Main menu
        while(!exit){

            System.out.println("Choose any option :");
            System.out.println("1. Add a new personn");
            System.out.println("2. Exit\n");

            String option = scanner.next();

            if (option.equals("1")) {
                //Id + BFF in order toc create a new profile
                ArrayList<String> friends = new ArrayList<String>();
                System.out.println("First Name :");
                String id = scanner.next();
                System.out.println("BFF :");
                String bestFriend = scanner.next();

                //Second menu
                while(!secondExit) {

                    //The user is free to add any other information in his profile
                    Put p = new Put(Bytes.toBytes(id));
                    p.add(Bytes.toBytes("friends"), Bytes.toBytes("BFF"), Bytes.toBytes(bestFriend));
                    table.put(p);

                    System.out.println("\nCurrent profile :" + id);
                    System.out.println("1. Add family name");
                    System.out.println("2. Add birthdate");
                    System.out.println("3. Add address");
                    System.out.println("4. Add phone number");
                    System.out.println("5. Add bio");
                    System.out.println("6. Add friend");
                    System.out.println("7. Exit profile and save\n");
                    String secondOption = scanner.next();
                    switch (Integer.parseInt(StringUtils.isNumeric(secondOption) ? secondOption : "0")) {

                        case 1:
                            System.out.println("Family Name :");
                            String familyName = scanner.next();
                            p.add(Bytes.toBytes("info"), Bytes.toBytes("familyName"), Bytes.toBytes(familyName));
                            table.put(p);
                            break;
                        case 2:
                            System.out.println("Birthday :");
                            String birthDate = scanner.next();
                            p.add(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes(birthDate));
                            table.put(p);
                            break;
                        case 3:
                            System.out.println("Address :");
                            String address = scanner.next();
                            p.add(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(address));
                            break;
                        case 4:
                            System.out.println("Phone Number :");
                            String phoneNumber = scanner.next();
                            p.add(Bytes.toBytes("info"), Bytes.toBytes("phoneNumber"), Bytes.toBytes(phoneNumber));
                            table.put(p);
                            break;
                        case 5:
                            System.out.println("Bio :");
                            String bio = scanner.next();
                            p.add(Bytes.toBytes("info"), Bytes.toBytes("description"), Bytes.toBytes(bio));
                            table.put(p);
                            break;
                        case 6:
                            System.out.println("Friend name :");
                            String newFriend = scanner.next();
                            friends.add(newFriend);
                            table.put(p);
                            break;
                        case 7:
                            if(!friends.isEmpty()){
                                p.add(Bytes.toBytes("friends"), Bytes.toBytes("friendList"), WritableUtils.toByteArray(toWritable(friends)));
                            }
                            table.put(p);
                            friends.clear();
                            secondExit = true;
                            break;
                        default:
                            System.out.println("This option is not available");
                            break;

                    }
                }

            } else if (option.equals("2")) {
                exit = true;

            } else {
                System.out.println("Option not available");

            }


        }

    }

}

