package com.asiainfo.sdtp;

import java.lang.Thread.UncaughtExceptionHandler;

public class MyUncaughtExceptionHandler implements UncaughtExceptionHandler {  
	  
    @Override  
    public void uncaughtException(Thread t, Throwable e) {  
        System.out.printf("Uncaught exception raised and captured in thread  %s : \n", t.getName());  
    }  
  
}  
