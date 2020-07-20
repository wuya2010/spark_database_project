package com.alibaba.publish.service;

import com.alibaba.publish.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kylinWang
 * @data 2020/7/10 7:05
 */
public class PublisherServiceImpl implements PublishService{

    @Autowired
    DauMapper dauMapper;//自动注入

    @Override
    public long getDauTotal(String date) {
        return dauMapper.getDauToal(date);
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> daouHourList = dauMapper.getDauHour(date);
        HashMap daouHourMap = new HashMap();

        for (Map map : daouHourList) {
            String hour = (String)map.get("LOGHOUR");
            Long count = (Long) map.get("COUNT");
            daouHourMap.put(hour, count);
        }

        return daouHourMap;

    }





    //实现新的需求
    @Override
    public double getOrderAmountTotal(String date) {
        return 0;
    }

    @Override
    public Map getOrderAmountHour(String date) {
        return null;
    }
}
