package org.mappingnode.idmapping.utils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * <p>Date与 java 8 LocalDateTime LocalDate 之间的互相转化 工具方法</p>
 * @author panliyong  2017/9/4 18:47
 */
public class DateUtils {

    public static final String PATTERN_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    public static final String PATTERN_YYYYMMDDHHMMSSSSS = "yyyyMMddHHmmssSSS";
    public static final String PATTERN_YYYYMMDD = "yyyyMMdd";

    /**
     * <p>java.util.Date --> java.time.LocalDateTime</p>
     * @param date 需要转换的 时间
     * @return java.time.LocalDateTime
     * @author panliyong  2018/8/1 10:03
     */
    public static LocalDateTime date2LocalDataTime(Date date) {
        if (date != null) {
            Instant instant = date.toInstant();
            ZoneId zone = ZoneId.systemDefault();
            return LocalDateTime.ofInstant(instant, zone);
        } else {
            return null;
        }
    }

    /**
     * <p>java.time.LocalDateTime --> java.util.Date</p>
     * @param localDateTime 需要转换的 时间
     * @return java.util.Date
     * @author panliyong  2018/8/1 10:02
     */
    public static Date localDateTime2Date(LocalDateTime localDateTime) {
        if (localDateTime != null) {
            ZoneId zone = ZoneId.systemDefault();
            Instant instant = localDateTime.atZone(zone).toInstant();
            return Date.from(instant);
        } else {
            return null;
        }
    }

    /**
     * <p> 比较两个LocalTime的大小,大于等于返回true</p>
     * @param bigTime   大时间
     * @param smallTime 小时间
     * @return boolean
     * @author youq  2018/3/9 20:02
     */
    public static boolean compareTime(LocalTime bigTime, LocalTime smallTime) {
        int compare = bigTime.compareTo(smallTime);
        return compare >= 0;
    }

    /**
     * <p>java.util.Date --> java.time.LocalDate</p>
     * @author panliyong  2018/1/16 10:04
     */
    public static LocalDate date2LocalDate(Date date) {
        if (date != null) {
            Instant instant = date.toInstant();
            ZoneId zone = ZoneId.systemDefault();
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
            return localDateTime.toLocalDate();
        } else {
            return null;
        }
    }

    /**
     * <p> java.time.LocalDate --> java.util.Date</p>
     * @author panliyong  2018/1/16 10:08
     */
    public static Date localDate2Date(LocalDate localDate) {
        if (localDate != null) {
            ZoneId zone = ZoneId.systemDefault();
            Instant instant = localDate.atStartOfDay().atZone(zone).toInstant();
            return Date.from(instant);
        } else {
            return null;
        }
    }

    /**
     * <p>java.time.LocalTime --> java.util.Date</p>
     * @author panliyong  2018/1/16 10:10
     */
    public static Date localDateTimeAndLocalTime2date(LocalDate localDate, LocalTime localTime) {
        if (localDate != null && localTime != null) {
            LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
            ZoneId zone = ZoneId.systemDefault();
            Instant instant = localDateTime.atZone(zone).toInstant();
            return Date.from(instant);
        } else {
            return null;
        }
    }

    /**
     * <p> LocalDateTime转换成时间戳（毫秒）</p>
     * @author youq  2018/3/15 10:17
     */
    public static long localDateTimeToMillisecond(LocalDateTime localDateTime) {
        if (localDateTime != null) {
            return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        } else {
            return 0;
        }
    }

    /**
     * <p>long 秒值 --> LocalDateTime</p>
     * @author panliyong  2018/1/16 10:10
     */
    public static LocalDateTime second2LocalDateTime(Long seconds) {
        if (seconds > 0) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds), ZoneOffset.of("+8"));
        } else {
            return null;
        }
    }

    /**
     * <p>long 毫秒 --> LocalDateTime</p>
     * @author panliyong  2018/1/16 10:10
     */
    public static LocalDateTime millisecond2LocalDateTime(Long millisecond) {
        if (millisecond > 0) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millisecond), ZoneOffset.of("+8"));
        } else {
            return null;
        }
    }

    /**
     * <p>LocalDateTime --> long 秒值 </p>
     * @author panliyong  2018/1/16 10:10
     */
    public static long localDateTime2Second(LocalDateTime time) {
        if (time != null) {
            return time.toEpochSecond(ZoneOffset.of("+8"));
        } else {
            return 0;
        }
    }

    /**
     * <p>localDateTime 2 String </p>
     * @param time    需要转化的时间
     * @param pattern 转化的 格式
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static String localDateTimeFormat(LocalDateTime time, String pattern) {
        if (Objects.nonNull(time)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
            return df.format(time);
        } else {
            return "";
        }
    }

    /**
     * <p>localDate  2 String </p>
     * @param date    需要转化的日期
     * @param pattern 转化的 格式
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static String localDateTimeFormat(LocalDate date, String pattern) {
        if (Objects.nonNull(date)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
            return df.format(date);
        } else {
            return "";
        }
    }

    /**
     * <p>localTime 2 String </p>
     * @param time    需要转化的时间
     * @param pattern 转化的 格式
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static String localTimeFormat(LocalTime time, String pattern) {
        if (Objects.nonNull(time)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
            return df.format(time);
        } else {
            return "";
        }
    }

    /**
     * <p>localDateTime 2 yyyy-MM-dd HH:mm:ss String </p>
     * @param time 需要转化的时间
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static String localDateTimeDefaultFormat(LocalDateTime time) {
        if (Objects.nonNull(time)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            return df.format(time);
        } else {
            return "";
        }
    }

    /**
     * <p>localDate 2 yyyy-MM-dd  String </p>
     * @param date 需要转化的日期
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static String localDateDefaultFormat(LocalDate date) {
        if (Objects.nonNull(date)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            return df.format(date);
        } else {
            return "";
        }
    }

    /**
     * <p>localDate 2  HH:mm:ss String </p>
     * @param time 需要转化的时间
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static String localTimeDefaultFormat(LocalTime time) {
        if (Objects.nonNull(time)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern("HH:mm:ss");
            return df.format(time);
        } else {
            return "";
        }
    }

    /**
     * <p>yyyy-MM-dd HH:mm:ss String to LocalDateTime</p>
     * @param dateTimeStr 需要转化的时间
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static LocalDateTime string2LocalDateTime(String dateTimeStr) {
        try {
            if (Objects.nonNull(dateTimeStr)) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return LocalDateTime.parse(dateTimeStr, df);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * <p>yyyy-MM-dd HH:mm:ss String to LocalDateTime</p>
     * @param dateTimeStr 需要转化的时间
     * @return java.lang.String
     * @author panliyong  2018/7/4 16:00
     */
    public static LocalDateTime string2LocalDateTime(String dateTimeStr, String pattern) {
        try {
            if (Objects.nonNull(dateTimeStr)) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
                return LocalDateTime.parse(dateTimeStr, df);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalTime string2LocalTime(String dateTimeStr, String pattern) {
        try {
            if (Objects.nonNull(dateTimeStr)) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
                return LocalTime.parse(dateTimeStr, df);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalDate string2LocalDate(String dateDateStr) {
        try {
            if (Objects.nonNull(dateDateStr)) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return LocalDate.parse(dateDateStr, df);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalTime second2LocalTime(Long seconds) {
        return LocalTime.ofSecondOfDay(seconds);
    }


    /**
     * date to string
     * @param date
     * @return
     */
    public static String date2String(Date date){
        try {
            if (Objects.nonNull(date)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.format(date);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * date to string
     * @param date
     * @return
     */
    public static String date2StringPoint(Date date){
        try {
            if (Objects.nonNull(date)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
                return sdf.format(date);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获取时间差
     * @param end
     * @param start
     * @return
     */
    public static long getLongTime(LocalDateTime end, LocalDateTime start) {
        return Duration.between(start, end).getSeconds();
    }

    /**
     * <p>将 localTime 转成对应的 毫秒值</p>
     * @param localTime 需要转换到 localTime
     * @return long 对应 的毫秒值
     * @author panliyong  2019-08-03 16:57
     */
    public static long localTime2MillLong(LocalTime localTime) {
        if (Objects.nonNull(localTime)) {
            int hour = localTime.getHour();
            int minute = localTime.getMinute();
            int second = localTime.getSecond();
            return (hour * 60 * 60 * 1000) + (minute * 60 * 1000) + (second * 1000);
        } else {
            return 0L;
        }
    }

    /**
     * <p>将 毫秒值 转成对应的 LocalTime</p>
     * @param millSecond 需要转换到 毫秒值
     * @return LocalTime 对应 localTime
     * @author panliyong  2019-08-03 16:57
     */
    public static LocalTime millLong2LocalTime(long millSecond) {
        long hour = millSecond / (60 * 60 * 1000);
        long minute = (millSecond - (hour * 60 * 60 * 1000)) / (60 * 1000);
        long second = (millSecond - (hour * 60 * 60 * 1000) - minute * (60 * 1000)) / 1000;
        return LocalTime.of((int) hour, (int) minute, (int) second);

    }

    /**
     * <p>LocalDate转毫秒值</p>
     * @param localDate 时间
     * @return long 毫秒值
     * @author limm  2019/8/6 16:17
     */
    public static long localDate2MilliSecond(LocalDate localDate) {
        if (Objects.nonNull(localDate)) {
            return localDate.atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
        }
        return 0;
    }

    /**
     * <p>毫秒值转LocalDate</p>
     * @param milliSecond 毫秒值
     * @return java.time.LocalDate
     * @author limm  2019/8/6 16:17
     */
    public static LocalDate milliSecond2LocalDate(long milliSecond) {
        if (milliSecond > 0) {
            return Instant.ofEpochMilli(milliSecond).atZone(ZoneOffset.ofHours(8)).toLocalDate();
        }
        return null;
    }

    /**
     * <p>获取2时间相隔天数</p>
     * @param begin 开始时间
     * @param end   结束时间
     * @return long 天数
     * @author limm  2019/8/15 14:31
     */
    public static long date2DvalueDate(LocalDate begin, LocalDate end) {
        if (Objects.isNull(begin) || Objects.isNull(end)) {
            return 0;
        }
        return ChronoUnit.DAYS.between(begin, end) < 0 ? 0 : ChronoUnit.DAYS.between(begin, end);

    }

    /**
     * <p>格式化时间.秒值-->日期</p>
     * @param callDuration 秒值
     * @return java.lang.String
     * @author limm  2019/11/1 15:26
     */
    public static String millisecond2Duration(Long callDuration) {
        if (Objects.isNull(callDuration) || new Long(0).equals(callDuration)) {
            return "00:00:00";
        }
        if (1000 > callDuration && 0 < callDuration) {
            return "00:00:01";
        }
        //将秒格式化成HH:mm:ss
        //这里应该用Duration更合理，但它不能格式化成字符串
        //而使用LocalTime，在时间超过24小时后格式化也会有问题（！）
        long hours = callDuration / 1000 / 3600;

        long rem = callDuration / 1000 % 3600;
        long minutes = rem / 60;
        long seconds = rem % 60;

        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    /**
     * <p> 秒转时分秒</p>
     * @param second 秒
     * @return java.lang.String
     * @author youq  2020/4/10 13:04
     */
    public static String second2Time(Long second) {
        if (second != null && second > 0) {
            if (second < 60) {//秒
                return NumFormat(0) + ":" + NumFormat(second);
            }
            if (second < 3600) {//分
                return NumFormat(second / 60) + ":" + NumFormat(second % 60);
            }
            if (second < 3600 * 24) {//时
                return NumFormat(second / 60 / 60) + ":" + NumFormat(second / 60 % 60) + ":" + NumFormat(second % 60);
            }
            if (second >= 3600 * 24) {//天
                return NumFormat(second / 60 / 60 / 24) + "天" + NumFormat(second / 60 / 60 % 24) + ":" + NumFormat(second / 60 % 60) + ":" + NumFormat(second % 60);
            }
        }
        return "--";
    }

    public static String NumFormat(long i) {
        if (String.valueOf(i).length() < 2) {
            return "0" + i;
        } else {
            return String.valueOf(i);
        }
    }


    /**
     * 验证码有效截止日期时间
     * @param validityPeriod 指定有效时间
     * @return
     */
    public static Date getDefaultCaptchaValidDate(Integer validityPeriod) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, validityPeriod);
        return c.getTime();
    }

    /**
     * 或者之后的几个月
     * @param date
     * @param num
     * @return
     */
    public static Date  getLaterMonth(Date date,int num){
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MONTH, num);
        return c.getTime();
    }

    /**
     * 获取GMT时间(格林时间)
     * @return
     */
    public static String getGMTTime() {
        Calendar cd = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("EEE,d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // 设置时区为GMT
        return sdf.format(cd.getTime());
    }

    /**
     * 获取GMT时间(格林时间)
     * @return
     */
    public static String getGMTTimeEN() {
        Calendar cd = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("EEE,d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // 设置时区为GMT
        return sdf.format(cd.getTime());
    }

    /**
     * 获取GMT时间(格林时间)
     * @return
     */
    public static Date getGMTDate(String dateTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE,d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // 设置时区为GMT
        try {
            return sdf.parse(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 时间加减
     * @return
     */
    public static Date add(Date date, int type, int val) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(type, val);
        return c.getTime();
    }

    /**
     * 获取某一天到结束时剩余分钟
     * @return
     */
    public static int getToDayEndMinute(){
        Calendar end = Calendar.getInstance();
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 55);
        Calendar now = Calendar.getInstance();
        return (int)((end.getTimeInMillis() - now.getTimeInMillis()) / 1000 / 60);
    }

    /**
     * 获取某一周到结束时剩余分钟
     * @return
     */
    public static int getToWeekEndMinute(){
        Calendar end = Calendar.getInstance();
        end.setFirstDayOfWeek(Calendar.MONDAY);
        end.setTime(new Date());
        end.set(Calendar.DAY_OF_WEEK, end.getFirstDayOfWeek() + 6);
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 55);
        Calendar now = Calendar.getInstance();
        return (int)((end.getTimeInMillis() - now.getTimeInMillis()) / 1000 / 60);
    }

    /**
     * 获取某一月到结束时剩余分钟
     * @return
     */
    public static int getToMonthEndMinute(){
        Calendar end = Calendar.getInstance();
        end.set(Calendar.DAY_OF_MONTH, 1);
        end.set(Calendar.DATE, end.getActualMaximum(Calendar.DATE));
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 55);
        Calendar now = Calendar.getInstance();
        return (int)((end.getTimeInMillis() - now.getTimeInMillis()) / 1000 / 60);
    }


    /**
     * 获取当天结束
     * @return
     */
    public static Date getDayEnd(){
        Calendar end = Calendar.getInstance();
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 55);
        return end.getTime();
    }

    /**
     * 获取本周结束
     * @return
     */
    public static Date getWeekEnd(){
        Calendar end = Calendar.getInstance();
        end.setFirstDayOfWeek(Calendar.MONDAY);
        end.setTime(new Date());
        end.set(Calendar.DAY_OF_WEEK, end.getFirstDayOfWeek() + 6);
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 55);
        return end.getTime();
    }

    /**
     * 获取本月结束
     * @return
     */
    public static Date getMonthEnd(){
        Calendar end = Calendar.getInstance();
        end.set(Calendar.DAY_OF_MONTH, 1);
        end.set(Calendar.DATE, end.getActualMaximum(Calendar.DATE));
        end.set(Calendar.HOUR, 23);
        end.set(Calendar.MINUTE, 59);
        end.set(Calendar.SECOND, 55);
        return end.getTime();
    }

    /**
     * 获取当天小时
     * @return
     */
    public static int getDayHour(){
        Calendar end = Calendar.getInstance();
        end.setFirstDayOfWeek(Calendar.MONDAY);
        end.setTime(new Date());
        return end.get(Calendar.HOUR_OF_DAY);
    }
}
