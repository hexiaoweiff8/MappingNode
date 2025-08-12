package org.mappingnode.idmapping.db;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.Resource;
import javax.sql.DataSource;

@EnableTransactionManagement
@Configuration
public class DbConfig {
	@Resource(name = "myRoutingDataSource")
	private DataSource myRoutingDataSource;


	@Bean
	public PlatformTransactionManager platformTransactionManager() {
		return new DataSourceTransactionManager(myRoutingDataSource);
	}

}
