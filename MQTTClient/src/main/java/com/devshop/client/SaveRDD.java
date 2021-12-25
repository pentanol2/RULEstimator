/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;


/**
 *
 * @author DevShop
 */

public class SaveRDD implements Function<JavaRDD<String>,Void> {

    @Override
    public Void call(JavaRDD<String> sdata) throws Exception {
        long size =  sdata.count();
        JavaRDD<String> fsdata = null; // a formatted sensor data RDD
        System.out.println(" == >"+size);
        if (size >= 0){
            //List<String> lines = sdata.toArray();
            //for (int i = 0;i<size;i++){
            //    System.out.println(""+lines.get(i));
            //} 
            //System.out.println("1st "+sdata.first());
            //System.out.println("There is a signal!");
        }
        
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return null;
    }

    

}
