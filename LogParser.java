package com.javarush.task.task39.task3913;

import com.javarush.task.task39.task3913.query.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {
    MyFileVisitor fileVisitor = new MyFileVisitor();
    List<MyLog> logs;
    public LogParser(Path logDir){

       this.logs = new ArrayList<>();
        try {
            Files.walkFileTree(logDir, fileVisitor);
            for (String s :fileVisitor.log
                 ) {
                logs.add(new MyLog(s));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
//        List<MyLog> myLogList = getAfterBefore(after, before);
//        List<String> idList = myLogList
//                .stream()
//                .map(p -> p.ip)
//                .distinct()
//                .collect(Collectors.toList());
//        return idList.size();
       return getUniqueIPs(after, before).size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        List<MyLog> myLogList = getAfterBefore(after, before);
        Set<String> idList = myLogList
                .stream()
                .map(p -> p.ip)
                .distinct()
                .collect(Collectors.toSet());
        return idList;

    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        List<MyLog> myLogList = getAfterBefore(after, before);
       Set<String> ipUser = myLogList
               .stream()
              .filter(p -> p.user.equals(user))
               .map(p-> p.ip)
               .collect(Collectors.toSet());
        return ipUser;
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        List<MyLog> myLogList = getAfterBefore(after, before);
        Set<String> ipEvent = myLogList
                .stream()
                .filter(p -> p.event.equals(event))
                .map(p-> p.ip)
                .collect(Collectors.toSet());
        return ipEvent;

    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        List<MyLog> myLogList = getAfterBefore(after, before);
        Set<String> ipStatus = myLogList
                .stream()
                .filter(p -> p.status.equals(status))
                .map(p-> p.ip)
                .collect(Collectors.toSet());
        return ipStatus;
    }


    public List<MyLog> getAfterBefore(Date after, Date before) {
        List<MyLog> result = null;
        if(after==null && before == null){
            result = logs;
        }
        else if(after == null ){
             result = logs.
                    stream()
                    .filter(m -> m.date.before(before))
                    .collect(Collectors.toList());
        }
        else if (before == null){
            result = logs.
                    stream()
                    .filter(m -> m.date.after(after))
                    .collect(Collectors.toList());
        }
        else {

            result = logs.stream()
                    .filter((m) -> m.date.after(after)
                    && m.date.before(before))

                    .collect(Collectors.toList());

        }
        return  result;
    }

    @Override
    public Set<String> getAllUsers() {
        return logs.stream()
                .map(p -> p.user)

                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s.size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        List<MyLog>l = getAfterBefore(after, before);
        Set<Event> s = l.stream()
                .filter(p-> p.user.equals(user))
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s.size();
    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> sIp = l.stream()
                .filter(p -> p.ip.equals(ip))
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return sIp;
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter(p -> p.event.equals(Event.LOGIN))
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s;

    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter(p -> p.event.equals(Event.DOWNLOAD_PLUGIN))
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter(p -> p.event.equals(Event.WRITE_MESSAGE))
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter(p -> p.event.equals(Event.SOLVE_TASK))
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter((p) -> p.event.equals(Event.SOLVE_TASK) && p.task==task)
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter(p -> p.event.equals(Event.DONE_TASK))
                .map(p -> p.user)

                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<String> s = l.stream()
                .filter((p) -> p.event.equals(Event.DONE_TASK) && p.task==task)
                .map(p -> p.user)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Date> s = l.stream()
                .filter((p) -> p.user.equals(user) && p.event.equals(event))
                .map(p -> p.date)
                .collect(Collectors.toSet());
        return s;

    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Date> s = l.stream()
                .filter((p) -> p.status.equals(Status.FAILED))
                .map(p -> p.date)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Date> s = l.stream()
                .filter((p) -> p.status.equals(Status.ERROR))
                .map(p -> p.date)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
try {
    Date s = l.stream()
            .filter((p) -> p.user.equals(user) && p.event.equals(Event.LOGIN))
            .map(p -> p.date)
            .min(Date::compareTo)
            .get();
    return s;
}
catch (Exception e){
    return null;
}


    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        try {
            Date s = l.stream()
                    .filter((p) -> p.user.equals(user) && p.event.equals(Event.SOLVE_TASK) && p.task == task)
                    .map(p -> p.date)
                    .min(Date::compareTo)
                    .get();
            return s;
        }
        catch (Exception e){
            return null;
        }
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        try {
            Date s = l.stream()
                    .filter((p) -> p.user.equals(user) && p.event.equals(Event.DONE_TASK) && p.task == task)
                    .map(p -> p.date)
                    .min(Date::compareTo)
                    .get();
            return s;
        }
        catch (Exception e){
            return null;
        }
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Date> s = l.stream()
                .filter((p) -> p.user.equals(user) && p.event.equals(Event.WRITE_MESSAGE))
                .map(p -> p.date)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Date> s = l.stream()
                .filter((p) -> p.user.equals(user) && p.event.equals(Event.DOWNLOAD_PLUGIN))
                .map(p -> p.date)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Event>s = l.stream()
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s.size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Event>s = l.stream()
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Event>s = l.stream()
                .filter(p -> p.ip.equals(ip))
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Event>s = l.stream()
                .filter(p -> p.user.equals(user))
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Event>s = l.stream()
                .filter(p -> p.status.equals(Status.FAILED))
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Event>s = l.stream()
                .filter(p -> p.status.equals(Status.ERROR))
                .map(p -> p.event)
                .collect(Collectors.toSet());
        return s;
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        List<MyLog>n = l.stream()
                .filter((p) -> p.task == task && p.event.equals(Event.SOLVE_TASK))
                .collect(Collectors.toList());
        return n.size();
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        List<MyLog>n = l.stream()
                .filter((p) -> p.task == task && p.event.equals(Event.DONE_TASK) )
                .collect(Collectors.toList());
        return n.size();
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Integer>taskSet = l.stream()
                .filter(p -> p.event.equals(Event.SOLVE_TASK))
                .map(p -> p.task)
                .collect(Collectors.toSet());
        Map<Integer, Integer> map = new HashMap<>();
        for (Integer task : taskSet
             ) {
            Long d = l.stream()
                    .filter((p) ->p.event.equals(Event.SOLVE_TASK) && p.task == task )
                    .count();
            map.put(task, d.intValue());
        }
        return map;
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        List<MyLog> l = getAfterBefore(after, before);
        Set<Integer>taskSet = l.stream()
                .filter(p -> p.event.equals(Event.DONE_TASK))
                .map(p -> p.task)
                .collect(Collectors.toSet());
        Map<Integer, Integer> map = new HashMap<>();
        for (Integer task : taskSet
        ) {
            Long d = l.stream()
                    .filter((p) ->p.event.equals(Event.DONE_TASK) && p.task == task )
                    .count();
            map.put(task, d.intValue());
        }
        return map;
    }

    @Override
    public Set<Object> execute(String query) {
        String[] s = query.split(" ");
        String param = s[1];

        List<MyLog> res = new ArrayList<>();
        if(s.length > 2) {
            String param2 = s[3];
            String[] value = query.split("\"");
            String val = value[1];


            if (param2.equals("user")) {
                res = logs.stream()
                        .filter(p -> p.user.equals(val))
                        .collect(Collectors.toList());
            }
            if (param2.equals("ip")) {
                res = logs.stream()
                        .filter(p -> p.ip.equals(val))
                        .collect(Collectors.toList());
            }

            if (param2.equals("date")) {
                Date d = null;
                try {
                    d = new SimpleDateFormat("d.M.y H:m:s").parse(val);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                final Date ds = d;

                res = logs.stream()
                        .filter(p -> p.date.compareTo(ds)==0)
                        .collect(Collectors.toList());
            }

            if (param2.equals("event")) {
                Event event = null;
                switch (val) {
                    case ("LOGIN"):
                        event = Event.LOGIN;
                        break;
                    case ("DOWNLOAD_PLUGIN"):
                        event = Event.DOWNLOAD_PLUGIN;
                        break;
                    case ("WRITE_MESSAGE"):
                        event = Event.WRITE_MESSAGE;
                        break;
                    case ("SOLVE_TASK"):
                        event = Event.SOLVE_TASK;
                        break;
                    case ("DONE_TASK"):
                        event = Event.DONE_TASK;
                        break;
                }
                final Event f = event;

                res = logs.stream()
                        .filter(p -> p.event.equals(f))
                        .collect(Collectors.toList());
            }

            if (param2.equals("status")) {
                Status status = null;
                switch (val) {
                    case ("OK"):
                        status = Status.OK;
                        break;
                    case ("FAILED"):
                        status = Status.FAILED;
                        break;
                    case ("ERROR"):
                        status = Status.ERROR;
                        break;
                }
                final Status f = status;

                res = logs.stream()
                        .filter(p -> p.status.equals(f))
                        .collect(Collectors.toList());


            }
        }
            else {
                res = logs;
            }


        Set<Object> result =null;
        if(param.equals("user")){
            result = res.stream()
                    .map(p -> (Object)p.user)
                    .collect(Collectors.toSet());
        }
        if(param.equals("ip")){
            result = res.stream()
                    .map(p -> (Object)p.ip)
                    .collect(Collectors.toSet());
        }
        if(param.equals("date")){
            result = res.stream()
                    .map(p -> (Object)p.date)
                    .collect(Collectors.toSet());
        }
        if(param.equals("event")){
            result = res.stream()
                    .map(p -> (Object)p.event)
                    .collect(Collectors.toSet());
        }
        if(param.equals("status")){
            result = res.stream()
                    .map(p -> (Object)p.status)
                    .collect(Collectors.toSet());
        }







        return result;
    }
}