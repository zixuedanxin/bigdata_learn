package com.yanxml.kafka.stresstest.config;

import java.beans.PropertyVetoException;

import javax.sql.DataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;


public class DBConfig {
	
	public static DataSource datasource = null;
	
	static {
		try {
			datasource = dataSource();
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}

    public static ComboPooledDataSource dataSource() throws PropertyVetoException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass(StressEnvConfig.jdbc_driverClassName);
        dataSource.setJdbcUrl(StressEnvConfig.jdbc_url);
        dataSource.setUser(StressEnvConfig.jdbc_username);
        dataSource.setPassword(StressEnvConfig.jdbc_password);
        dataSource.setMaxPoolSize(Integer.parseInt(StressEnvConfig.jdbc_connection_maxPoolSize));
        dataSource.setMinPoolSize(Integer.parseInt(StressEnvConfig.jdbc_connection_minPoolSize));
        dataSource.setInitialPoolSize(Integer.parseInt(StressEnvConfig.jdbc_connection_initialPoolSize));
        dataSource.setMaxIdleTime(1000);
        dataSource.setAcquireIncrement(2);
        dataSource.setIdleConnectionTestPeriod(60);

        return dataSource;
    }
    
    public static DataSource getDataSource(){
    	return datasource;
    }

}
