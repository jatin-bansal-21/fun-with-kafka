package com.jatin.funwithkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class FunWithKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(FunWithKafkaApplication.class, args);
    }

}
