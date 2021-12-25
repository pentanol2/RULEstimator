/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author DevShop
 */
public class AnalyseBatch implements Function<JavaRDD<String>,Void> {

      
    public Void call(JavaRDD<String> sdata) throws Exception {
        long size =  sdata.count();
        JavaRDD<String> fsdata = null; // a formatted sensor data RDD
        System.out.println(" == >"+size);
        if (size >= 0){
            
        }
        
        
        return null;
    }

    
}
