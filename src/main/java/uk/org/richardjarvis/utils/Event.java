package uk.org.richardjarvis.utils;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class Event implements Serializable {

    public static final String SEPARATOR = ",";
    private static final ZoneOffset zoneOffset = ZoneOffset.UTC;
    private String id;
    private String eventType;
    private LocalDateTime date;
    private int hourOfDay;
    private List<String> windowList = new ArrayList<>();
    private String day;
    private String weekday;
    private String month;
    private double amount;
    private double size;
    private String dayOrNight;


    public Event() {
    }

    public Event(String id, String eventType, long date, double amount, double size) {

        this.id = id;
        this.eventType = eventType;
        this.amount = amount;
        this.size = size;
        setDate(date);
    }

    public static List<String> getTimeWindowMethods() {

        List<String> methods = new ArrayList<>();

        methods.add("getHourOfDay");
        methods.add("getDay");
        methods.add("getDayOrNight");
        methods.add("getWeekday");
        methods.add("getDay");
        methods.add("getMonth");

        return methods;
    }

    public String getDayOrNight() {
        return dayOrNight;
    }

    public void setDayOrNight(String dayOrNight) {
        this.dayOrNight = dayOrNight;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getDate() {
        return date.toEpochSecond(zoneOffset);
    }

    public void setDate(long date) {
        setDate(LocalDateTime.ofEpochSecond(date / 1000, 0, zoneOffset));
    }

    public void setDate(LocalDateTime date) {

        this.date = date;
        this.day = date.getDayOfWeek().toString();
        this.month = date.getMonth().toString();
        this.hourOfDay = date.getHour();

        if (this.date.getDayOfWeek() == DayOfWeek.SATURDAY || this.date.getDayOfWeek() == DayOfWeek.SUNDAY) {
            this.weekday = "WEEKEND";
        } else {
            this.weekday = "WEEKDAY";
        }

        if (this.date.getHour() >= 8 && this.date.getHour() <= 7) {
            this.dayOrNight = "DAY";
        } else {
            this.dayOrNight = "NIGHT";
        }


        windowList.add(getDay());
        windowList.add(getDayOrNight());
        windowList.add(getHourOfDay());
        windowList.add(getWeekday());
        windowList.add(getMonth());

    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public double getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getDay() {
        return date.getDayOfWeek().toString();
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getWeekday() {
        return this.weekday;
    }

    public void setWeekday(String weekday) {
        this.weekday = weekday;
    }

    public String getHourOfDay() {
        return "HOUR_" + hourOfDay;
    }

    public int getHourOfDayInt() {
        return hourOfDay;
    }

    public String getMonth() {
        return date.getMonth().toString();
    }

    @Override
    public String toString() {
        return "Event{" +

                "id='" + id + '\'' +
                ", eventType='" + eventType + '\'' +
                ", date=" + date +
                ", hourOfDay='" + hourOfDay + '\'' +
                ", day='" + day + '\'' +
                ", weekday='" + weekday + '\'' +
                ", dayOrNight='" + dayOrNight + '\'' +
                ", amount=" + amount +
                ", size=" + size +
                '}';
    }

    public List<String> getWindowList() {

        return this.windowList;
    }
}
