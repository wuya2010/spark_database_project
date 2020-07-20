package com.alibaba.publish.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author kylinWang
 * @data 2020/7/8 7:49
 *  定义一个接口
 */
public interface DauMapper {
    //查询日活接口
    long getDauToal(String date);
    //查询小时明细
    List<Map> getDauHour(String date);
}
