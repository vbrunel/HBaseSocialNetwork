package HBase;
/**
 * Created by tosnos on 02/11/16.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.hadoop.io.*;
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
        Scan s = new Scan();
        boolean exit = false; //first REPL
        boolean secondExit = false; //second REPL

        //Main menu
        while(!exit){

            secondExit = false;
            System.out.println("\nChoose any option :");
            System.out.println("1. Add a new personn");
            System.out.println("2. Display information concerning one profile");
            System.out.println("3. Exit\n");

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
                System.out.println("First Name (id) :");
                String firstName = scanner.next();
                Get g = new Get(Bytes.toBytes(firstName));
                Result r = table.get(g);
                byte [] familyNameByte = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("familyName"));
                System.out.println("Family Name : "+Bytes.toString(familyNameByte));
                byte [] BFFByte = r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("BFF"));
                System.out.println("BFF : "+Bytes.toString(BFFByte));
                byte [] birthdateByte = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("birthday"));
                System.out.println("Birthday : "+Bytes.toString(birthdateByte));
                byte [] addressByte = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("address"));
                System.out.println("Address : "+Bytes.toString(addressByte));
                byte [] phoneNumberByte = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("phoneNumber"));
                System.out.println("Phone Number : "+Bytes.toString(phoneNumberByte));
                byte [] descriptionByte = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("description"));
                System.out.println("Bio : "+Bytes.toString(descriptionByte));

                try{
                    ArrayWritable friendsListWritable = new ArrayWritable(Text.class);
                    friendsListWritable.readFields(
                            new DataInputStream(
                                    new ByteArrayInputStream(
                                            r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("friendList"))
                                    )
                            )
                    );
                    ArrayList<String> friendsList = fromWritable(friendsListWritable);
                    System.out.println("Friends " + friendsList.toString());
                } catch (NullPointerException e) {}


            } else if (option.equals("3")) {
                exit = true;

            /***} else if (option.equals("4")) {

                try {
                s.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("friendList"));
                ResultScanner resScan = table.getScanner(s);

                    for (Result rr = resScan.next(); rr != null; rr = resScan.next()) {
                        System.out.println("Found row: " + rr);
                    }

                } catch (IllegalStateException e) {}
                finally {
                    scanner.close();
                }***/

            } else {
                System.out.println("Option not available");

            }


        }

    }

}

