/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTInputDStream;
import org.apache.spark.streaming.mqtt.MQTTReceiver;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


/**
 *
 * @author DevShop
 */
public class SparkStreamer {

    public static void main(String[] args) throws InterruptedException {
        String brokerUrl = "tcp://cookbae.com:1883";
        String topics = "sensor1";
        SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("NetworkWordCount");
        //Logger.getLogger("org").setLevel(Level.WARN);
        //Logger.getLogger("akka").setLevel(Level.WARN);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Minutes.apply(1));
        jssc.checkpoint("c:\\mygrad\\");
        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topics);
        messages.foreachRDD(new SaveRDD());
        System.out.println("We are here with you!");
        jssc.start();
        jssc.awaitTermination();

    }

}
