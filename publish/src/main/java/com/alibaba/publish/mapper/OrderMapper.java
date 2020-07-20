package com.alibaba.publish.mapper;

import java.util.*;


/**
 * @author kylinWang
 * @data 2020/7/10 7:46
 */
public interface OrderMapper {
    double getOrderAmoutTotal(String date);

    List<Map> getOrderAmountHour(String date);
}
