package org.mappingnode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import tk.mybatis.spring.annotation.MapperScan;


@SpringBootApplication
@EnableScheduling
@ComponentScan(basePackages = {
        "org.mappingnode.*"
})
@MapperScan(basePackages = {
        "org.mappingnode.idmapping.mapper"
})
public class MappingNodeApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(MappingNodeApplication.class);
        application.addListeners(new ApplicationPidFileWriter("application.pid"));
        application.run(args);
    }
}