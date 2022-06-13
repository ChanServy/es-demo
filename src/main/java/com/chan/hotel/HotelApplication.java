package com.chan.hotel;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author CHAN
 * @since 2022/5/17
 */
@SpringBootApplication
@MapperScan("com.chan.hotel.mapper")
@EnableTransactionManagement
public class HotelApplication {
    public static void main(String[] args) {
        SpringApplication.run(HotelApplication.class, args);
    }
}
