package com.javarush.task.task39.task3913;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyLog {

   String ip; // адрес с которого пользователь произвел событие.
    String user; // имя пользователя (одно или несколько слов разделенные пробелами).
     Date date; // дата события в формате day.month.year hour:minute:second.
     Event event;
     Status status;
     int task = 0;

     public MyLog(String s){
         String[] ss = s.split("\t");
         this.ip = ss[0];
         this.user = ss[1];
         SimpleDateFormat format = new SimpleDateFormat("d.M.y H:m:s");
         try {
             this.date = format.parse(ss[2]);
         } catch (ParseException e) {
             e.printStackTrace();
         }
         String[] ss3 = ss[3].split(" ");

        switch (ss3[0]){
            case "LOGIN": this.event = Event.LOGIN;
            break;
            case "DOWNLOAD_PLUGIN": this.event = Event.DOWNLOAD_PLUGIN;
                break;
            case "WRITE_MESSAGE": this.event = Event.WRITE_MESSAGE;
                break;
            case "SOLVE_TASK": this.event = Event.SOLVE_TASK;
                break;
            case "DONE_TASK": this.event = Event.DONE_TASK;
                break;
        }
        if(event.equals(Event.DONE_TASK) || event.equals(Event.SOLVE_TASK)) {
            this.task = Integer.parseInt(ss3[1]);
        }

        switch ((ss[4])){
            case "OK": this.status = Status.OK;
                break;
            case "FAILED": this.status = Status.FAILED;
                break;
            case "ERROR": this.status = Status.ERROR;
                break;
        }
     }

    @Override
    public String toString() {
        return "MyLog{" +
                "ip='" + ip + '\'' +
                ", user='" + user + '\'' +
                ", date=" + date +
                ", event=" + event +
                ", status=" + status +
                '}' + "\n";
    }
}
