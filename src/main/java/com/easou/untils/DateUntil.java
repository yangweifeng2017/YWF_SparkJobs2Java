package com.easou.untils;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * ClassName DateUntil
 * 功能: TODO
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2018/11/20 17:23
 * Version 1.0
 **/
public class DateUntil {
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    public static SimpleDateFormat simpleDateFormat_hour = new SimpleDateFormat("yyyyMMddHH");
    /**
     * 获取一分钟之前
     * @return 2018110216
     */
    public static List<String> getDateOneMinuteAgo() {
        List<String> list = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.MINUTE, - 1);
        list.add(sdf.format(calendar.getTime()));
        calendar.add(Calendar.MINUTE, - 9);
        list.add(sdf.format(calendar.getTime()));
        return list;
    }
    /**
     * 获取一分钟之前秒
     * @return 2018110216
     */
    public static String getDateOneMinuteAgoSecond() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.MINUTE, - 1);
        return simpleDateFormat.format(calendar.getTime());
    }
    /**
     * 获取一分钟之前秒
     * @return 2018110216
     */
    public static String getDateOneHourAgo() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, - 1);
        return simpleDateFormat_hour.format(calendar.getTime());
    }

    public static void main(String[] args) {

        System.out.println(getDateOneHourAgo());
        System.out.println("2018112421".substring(0,8));
    }

}
