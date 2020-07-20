package com.alibaba.publish.service;

import java.util.Map;

/**
 * @author kylinWang
 * @data 2020/7/8 7:53
 *  服务接口
 */
public interface PublishService {
    //查询总数
    long getDauTotal(String date);
    // 查询小时明细
    Map getDauHour(String date);


    //销售额
    double getOrderAmountTotal(String date);
    //销售明细
    Map getOrderAmountHour(String date);
}
