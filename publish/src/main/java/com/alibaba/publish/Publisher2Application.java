package com.alibaba.publish;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author kylinWang
 * @data 2020/7/10 7:12
 */

@SpringBootApplication
// 增加扫描包 //todo: 作用是什么？
@MapperScan(basePackages = "com.atguigu.publisher.mapper")
public class Publisher2Application {
    public static void main(String[] args) {
        SpringApplication.run(Publisher2Application.class, args);
    }
}
