package com.javarush.task.task39.task3913;

import javax.management.Query;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;

public class Solution {
    public static void main(String[] args) {
        LogParser logParser = new LogParser(Paths.get("C:\\Users\\user\\Downloads\\JavaRushTasks\\4.JavaCollections\\src\\com\\javarush\\task\\task39\\task3913\\logs\\"));
Set<Object> v = logParser.execute("get ip for event = \"LOGIN\" and date between \"11.12.2013 0:00:00\" and \"03.01.2014 23:59:59\"");
        System.out.println(v);
         //   System.out.println(logParser.getAfterBefore(new GregorianCalendar(2020, 0 , 1).getTime(), new Date() ));


       // System.out.println(logParser.getNumberOfUniqueIPs(null, new Date()));
    }
}