package com.beifeng.test;

import java.awt.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by youzeng
 * time: 2018-09-24 16:47
 */
public class TimerTest {
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
//        executeOnce();
        executeLoop();

//        Date now = new Date();
//        SimpleDateFormat dfDate = new SimpleDateFormat("yyyy-MM-dd");
//        SimpleDateFormat dfTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        try {
//            Date nowDate = dfDate.parse(dfDate.format(now));
//            Calendar cStart = Calendar.getInstance();
//            cStart.setTime(nowDate);
//            cStart.add(Calendar.MINUTE, 0);
//            Date start = cStart.getTime();
//
//            Calendar cEnd = Calendar.getInstance();
//            cEnd.setTime(nowDate);
//            cEnd.add(Calendar.MINUTE, 5);
//            Date end = cEnd.getTime();
//            System.out.println(cStart == cEnd);
//
//            System.out.println(dfTime.format(start));
//            System.out.println(dfTime.format(end));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }


    }



    /**
     * 只执行一次
     */
    public static void executeOnce(){
        Timer timer = new Timer();
        String now = df.format(new Date());
        System.out.println(now);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("当前时间：" + df.format(new Date()));
            }
        }, 1000);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("当前时间：" + df.format(new Date()));
            }
        }, 1000);
    }

    /**
     * 循环执行
     */
    public static void executeLoop(){
        System.out.println(df.format(new Date()));
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
//                System.out.println("当前时间：" + df.format(new Date()));

                Date now = new Date();
                SimpleDateFormat dfDate = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat dfTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date nowDate = dfDate.parse(dfDate.format(now));
                    Calendar cStart = Calendar.getInstance();
                    cStart.setTime(nowDate);
                    cStart.add(Calendar.MINUTE, 53);
                    cStart.add(Calendar.HOUR, 19);
                    Date start = cStart.getTime();

                    Calendar cEnd = Calendar.getInstance();
                    cEnd.setTime(nowDate);
                    cEnd.add(Calendar.MINUTE, 54);
                    cEnd.add(Calendar.HOUR, 19);
                    Date end = cEnd.getTime();

                    System.out.println(dfTime.format(start));
                    System.out.println(dfTime.format(end));
                    if(now.after(start) && now.before(end)){
                        Calendar yesterday = Calendar.getInstance();
                        yesterday.add(Calendar.DAY_OF_MONTH, -1);
                        Date dYesterday = yesterday.getTime();
                        System.out.println("统计昨天的数据：" + dfDate.format(dYesterday));
                    }else{
                        System.out.println("统计当前时间的数据: " + dfDate.format(now));
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }, 0,10000);
    }
}
