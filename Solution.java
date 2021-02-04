package com.javarush.task.task39.task3913;

import javax.management.Query;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.GregorianCalendar;

public class Solution {
    public static void main(String[] args) {
        LogParser logParser = new LogParser(Paths.get("C:\\Users\\user\\Downloads\\JavaRushTasks\\4.JavaCollections\\src\\com\\javarush\\task\\task39\\task3913\\logs\\"));
logParser.execute("get event for date = \"03.01.2014 03:45:23\"");

         //   System.out.println(logParser.getAfterBefore(new GregorianCalendar(2020, 0 , 1).getTime(), new Date() ));


       // System.out.println(logParser.getNumberOfUniqueIPs(null, new Date()));
    }
}