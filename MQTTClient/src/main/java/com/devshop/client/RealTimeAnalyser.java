/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.spark.ml.PipelineModel;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.ClientBuilder;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

/**
 *
 * @author DevShop
 */
public class RealTimeAnalyser {

    public static class MUnit implements Serializable {

        int unit;
        int cycle;
        double sensor1;
        double sensor2;
        double sensor3;
        double sensor4;
        double sensor5;
        double sensor6;
        double sensor7;
        double sensor8;
        double sensor9;
        double sensor10;
        double sensor11;
        double sensor12;
        double sensor13;
        double sensor14;
        double sensor15;
        double sensor16;
        double sensor17;
        double sensor18;
        double sensor19;
        double sensor20;
        double sensor21;

        MUnit(String params) {
            processParams(params);
        }

        private void processParams(String params) {
            String[] tmp = params.split(" ");
            this.unit = Integer.parseInt(tmp[0]);
            this.cycle = Integer.parseInt(tmp[1]);
            this.sensor1 = Double.parseDouble(tmp[2]);
            this.sensor2 = Double.parseDouble(tmp[3]);
            this.sensor3 = Double.parseDouble(tmp[4]);
            this.sensor4 = Double.parseDouble(tmp[5]);
            this.sensor5 = Double.parseDouble(tmp[6]);
            this.sensor6 = Double.parseDouble(tmp[7]);
            this.sensor7 = Double.parseDouble(tmp[8]);
            this.sensor8 = Double.parseDouble(tmp[9]);
            this.sensor9 = Double.parseDouble(tmp[10]);
            this.sensor10 = Double.parseDouble(tmp[11]);
            this.sensor11 = Double.parseDouble(tmp[12]);
            this.sensor12 = Double.parseDouble(tmp[13]);
            this.sensor13 = Double.parseDouble(tmp[14]);
            this.sensor14 = Double.parseDouble(tmp[15]);
            this.sensor15 = Double.parseDouble(tmp[16]);
            this.sensor16 = Double.parseDouble(tmp[17]);
            this.sensor17 = Double.parseDouble(tmp[18]);
            this.sensor18 = Double.parseDouble(tmp[19]);
            this.sensor19 = Double.parseDouble(tmp[20]);
            this.sensor20 = Double.parseDouble(tmp[21]);
            this.sensor21 = Double.parseDouble(tmp[22]);

        }

    }

    public static void extractFeatures() {

    }

    public static void removeNose() {

    }

    public static void main(String[] args) throws InterruptedException, MalformedURLException {
        // we distable the Spark Logger 
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        // we establish a database connection
        databaseMod dbm = new databaseMod("EngineUnits");
        // we import the saved model
        //PipelineModel pm = PipelineModel.read().load("/home/pentanol/pipmod");
        // we connect to the mqtt broker in the address cookbae.com:1883
        String brokerUrl = "tcp://cookbae.com:1883";
        String topics = "eunit";
        // we create spark context
        SparkConf conf = new SparkConf().setMaster("cluster").setAppName("Real Time Analyser");
        // we create the spark sql context
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Engine Modelizer")
                .config("spark.sql.caseSensitive", "false")
                .getOrCreate();
        // we create an encoder to describe our data

        Encoder<MUnit> MUnitEncoder = Encoders.bean(MUnit.class);

        // we create the streamming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Minutes.apply(1));
        // we checkpoint the result the linux file system
        jssc.checkpoint("/home/pentanol/stremerlog");
        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topics);
        messages.foreachRDD(new Function<JavaRDD<String>, Void>() {
            public Void call(JavaRDD<String> sdata) throws Exception {
                long size = sdata.count();
                ArrayList<MUnit> UList = new ArrayList();
                Dataset<MUnit> ds = null;
                // ds.re
                JavaRDD<String> fsdata = null; // a formatted sensor data RDD
                System.out.println(" == >" + size);
                if (size >= 0) {
                    // do your work here

                    // format data
                    // feed data to prediction model
                    // save predicted result
                    //spark.createDataFrame(sdata, String);
                    for (String line : sdata.collect()) {
                        // tranform to numerical form
                        UList.add(new MUnit(line));

                    }
                    ds = spark.createDataset(UList, MUnitEncoder);
                    ds.show(10);
      
                    //System.out.println("Print this works" + topics);
                }
                // clean noise
                // feed to model for analyse
                // persist result to database
                //System.out.println("* " + line);
                return null;
            }
        }
        );

        System.out.println("We are here with you!");
        jssc.start();

        jssc.awaitTermination();

    }

}
