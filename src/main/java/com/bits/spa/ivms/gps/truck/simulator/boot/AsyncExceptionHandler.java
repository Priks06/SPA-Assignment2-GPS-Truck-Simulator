package com.bits.spa.ivms.gps.truck.simulator.boot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;

import java.lang.reflect.Method;

public class AsyncExceptionHandler implements AsyncUncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(AsyncExceptionHandler.class);

    @Override
    public void handleUncaughtException(Throwable throwable, Method method, Object... obj) {
        logger.error("Error occurred while doing async execution of Thread: " +
                Thread.currentThread().getName() + ", Method: " + method.getName() +
                ", Arguments: " + obj + " Failed due to ", throwable);
    }
}
