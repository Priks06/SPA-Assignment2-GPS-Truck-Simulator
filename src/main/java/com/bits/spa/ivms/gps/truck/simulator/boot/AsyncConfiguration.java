package com.bits.spa.ivms.gps.truck.simulator.boot;


import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@ComponentScan(basePackages = "com.bits.spa.ivms.gps.truck.simulator")
public class AsyncConfiguration implements AsyncConfigurer {

    private static final String STREAM_THREAD_NAME_PREFIX = "StreamAsync-";

    @Bean(name = "streamEvenHandlerTaskExecutor")
    public Executor streamEvenHandlerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(5);
        executor.setThreadNamePrefix(STREAM_THREAD_NAME_PREFIX);
        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new AsyncExceptionHandler();
    }
}


