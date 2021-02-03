package com.javarush.task.task39.task3913;

import com.javarush.task.task39.task3913.query.DateQuery;
import com.javarush.task.task39.task3913.query.EventQuery;
import com.javarush.task.task39.task3913.query.IPQuery;
import com.javarush.task.task39.task3913.query.UserQuery;

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

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery {
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
            Long  d = l.stream()
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
}