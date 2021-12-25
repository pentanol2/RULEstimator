/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.sjob1;

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
import org.apache.spark.ml.feature.VectorAssembler;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

/**
 *
 * @author DevShop
 */
public class RealTimeAnalyser {

    public static class MUnit implements Serializable {

        private int unit;
        private int cycle;
        private double opm1;
        private double opm2;
        private double opm3;
        private double sensor1;
        private double sensor2;
        private double sensor3;
        private double sensor4;
        private double sensor5;
        private double sensor6;
        private double sensor7;
        private double sensor8;
        private double sensor9;
        private double sensor10;
        private double sensor11;
        private double sensor12;
        private double sensor13;
        private double sensor14;
        private double sensor15;
        private double sensor16;
        private double sensor17;
        private double sensor18;
        private double sensor19;
        private double sensor20;
        private double sensor21;

        MUnit(String params) {
            processParams(params);
        }

        private void processParams(String params) {
            String[] tmp = params.split(" ");
            this.setUnit(Integer.parseInt(tmp[0]));
            System.out.println("The unit vqlue is " + this.unit);
            this.setCycle(Integer.parseInt(tmp[1]));

            this.setOpm1(Double.parseDouble(tmp[2]));

            this.setOpm2(Double.parseDouble(tmp[3]));

            this.setOpm3(Double.parseDouble(tmp[4]));

            this.setSensor1(Double.parseDouble(tmp[5]));

            this.setSensor2(Double.parseDouble(tmp[6]));

            this.setSensor3(Double.parseDouble(tmp[7]));

            this.setSensor4(Double.parseDouble(tmp[8]));

            this.setSensor5(Double.parseDouble(tmp[9]));

            this.setSensor6(Double.parseDouble(tmp[10]));

            this.setSensor7(Double.parseDouble(tmp[11]));

            this.setSensor8(Double.parseDouble(tmp[12]));

            this.setSensor9(Double.parseDouble(tmp[13]));

            this.setSensor10(Double.parseDouble(tmp[14]));

            this.setSensor11(Double.parseDouble(tmp[15]));

            this.setSensor12(Double.parseDouble(tmp[16]));

            this.setSensor13(Double.parseDouble(tmp[17]));

            this.setSensor14(Double.parseDouble(tmp[18]));

            this.setSensor15(Double.parseDouble(tmp[19]));

            this.setSensor16(Double.parseDouble(tmp[20]));

            this.setSensor17(Double.parseDouble(tmp[21]));

            this.setSensor18(Double.parseDouble(tmp[22]));

            this.setSensor19(Double.parseDouble(tmp[23]));

            this.setSensor20(Double.parseDouble(tmp[24]));

            this.setSensor21(Double.parseDouble(tmp[25]));

        }

        /**
         * @return the unit
         */
        public int getUnit() {
            return unit;
        }

        /**
         * @param unit the unit to set
         */
        public void setUnit(int unit) {
            this.unit = unit;
        }

        /**
         * @return the cycle
         */
        public int getCycle() {
            return cycle;
        }

        /**
         * @param cycle the cycle to set
         */
        public void setCycle(int cycle) {
            this.cycle = cycle;
        }

        /**
         * @return the opm1
         */
        public double getOpm1() {
            return opm1;
        }

        /**
         * @param opm1 the opm1 to set
         */
        public void setOpm1(double opm1) {
            this.opm1 = opm1;
        }

        /**
         * @return the opm2
         */
        public double getOpm2() {
            return opm2;
        }

        /**
         * @param opm2 the opm2 to set
         */
        public void setOpm2(double opm2) {
            this.opm2 = opm2;
        }

        /**
         * @return the opm3
         */
        public double getOpm3() {
            return opm3;
        }

        /**
         * @param opm3 the opm3 to set
         */
        public void setOpm3(double opm3) {
            this.opm3 = opm3;
        }

        /**
         * @return the sensor1
         */
        public double getSensor1() {
            return sensor1;
        }

        /**
         * @param sensor1 the sensor1 to set
         */
        public void setSensor1(double sensor1) {
            this.sensor1 = sensor1;
        }

        /**
         * @return the sensor2
         */
        public double getSensor2() {
            return sensor2;
        }

        /**
         * @param sensor2 the sensor2 to set
         */
        public void setSensor2(double sensor2) {
            this.sensor2 = sensor2;
        }

        /**
         * @return the sensor3
         */
        public double getSensor3() {
            return sensor3;
        }

        /**
         * @param sensor3 the sensor3 to set
         */
        public void setSensor3(double sensor3) {
            this.sensor3 = sensor3;
        }

        /**
         * @return the sensor4
         */
        public double getSensor4() {
            return sensor4;
        }

        /**
         * @param sensor4 the sensor4 to set
         */
        public void setSensor4(double sensor4) {
            this.sensor4 = sensor4;
        }

        /**
         * @return the sensor5
         */
        public double getSensor5() {
            return sensor5;
        }

        /**
         * @param sensor5 the sensor5 to set
         */
        public void setSensor5(double sensor5) {
            this.sensor5 = sensor5;
        }

        /**
         * @return the sensor6
         */
        public double getSensor6() {
            return sensor6;
        }

        /**
         * @param sensor6 the sensor6 to set
         */
        public void setSensor6(double sensor6) {
            this.sensor6 = sensor6;
        }

        /**
         * @return the sensor7
         */
        public double getSensor7() {
            return sensor7;
        }

        /**
         * @param sensor7 the sensor7 to set
         */
        public void setSensor7(double sensor7) {
            this.sensor7 = sensor7;
        }

        /**
         * @return the sensor8
         */
        public double getSensor8() {
            return sensor8;
        }

        /**
         * @param sensor8 the sensor8 to set
         */
        public void setSensor8(double sensor8) {
            this.sensor8 = sensor8;
        }

        /**
         * @return the sensor9
         */
        public double getSensor9() {
            return sensor9;
        }

        /**
         * @param sensor9 the sensor9 to set
         */
        public void setSensor9(double sensor9) {
            this.sensor9 = sensor9;
        }

        /**
         * @return the sensor10
         */
        public double getSensor10() {
            return sensor10;
        }

        /**
         * @param sensor10 the sensor10 to set
         */
        public void setSensor10(double sensor10) {
            this.sensor10 = sensor10;
        }

        /**
         * @return the sensor11
         */
        public double getSensor11() {
            return sensor11;
        }

        /**
         * @param sensor11 the sensor11 to set
         */
        public void setSensor11(double sensor11) {
            this.sensor11 = sensor11;
        }

        /**
         * @return the sensor12
         */
        public double getSensor12() {
            return sensor12;
        }

        /**
         * @param sensor12 the sensor12 to set
         */
        public void setSensor12(double sensor12) {
            this.sensor12 = sensor12;
        }

        /**
         * @return the sensor13
         */
        public double getSensor13() {
            return sensor13;
        }

        /**
         * @param sensor13 the sensor13 to set
         */
        public void setSensor13(double sensor13) {
            this.sensor13 = sensor13;
        }

        /**
         * @return the sensor14
         */
        public double getSensor14() {
            return sensor14;
        }

        /**
         * @param sensor14 the sensor14 to set
         */
        public void setSensor14(double sensor14) {
            this.sensor14 = sensor14;
        }

        /**
         * @return the sensor15
         */
        public double getSensor15() {
            return sensor15;
        }

        /**
         * @param sensor15 the sensor15 to set
         */
        public void setSensor15(double sensor15) {
            this.sensor15 = sensor15;
        }

        /**
         * @return the sensor16
         */
        public double getSensor16() {
            return sensor16;
        }

        /**
         * @param sensor16 the sensor16 to set
         */
        public void setSensor16(double sensor16) {
            this.sensor16 = sensor16;
        }

        /**
         * @return the sensor17
         */
        public double getSensor17() {
            return sensor17;
        }

        /**
         * @param sensor17 the sensor17 to set
         */
        public void setSensor17(double sensor17) {
            this.sensor17 = sensor17;
        }

        /**
         * @return the sensor18
         */
        public double getSensor18() {
            return sensor18;
        }

        /**
         * @param sensor18 the sensor18 to set
         */
        public void setSensor18(double sensor18) {
            this.sensor18 = sensor18;
        }

        /**
         * @return the sensor19
         */
        public double getSensor19() {
            return sensor19;
        }

        /**
         * @param sensor19 the sensor19 to set
         */
        public void setSensor19(double sensor19) {
            this.sensor19 = sensor19;
        }

        /**
         * @return the sensor20
         */
        public double getSensor20() {
            return sensor20;
        }

        /**
         * @param sensor20 the sensor20 to set
         */
        public void setSensor20(double sensor20) {
            this.sensor20 = sensor20;
        }

        /**
         * @return the sensor21
         */
        public double getSensor21() {
            return sensor21;
        }

        /**
         * @param sensor21 the sensor21 to set
         */
        public void setSensor21(double sensor21) {
            this.sensor21 = sensor21;
        }

    }

    public static Dataset<?> mormalizeAndSelect(Dataset<?> ds) {
        Dataset<?> aux;
        VectorAssembler va = new VectorAssembler().setInputCols(new String[]{
            "sensor1", "sensor2", "sensor3", "sensor4", "sensor5",
            "sensor6", "sensor7", "sensor8", "sensor9", "sensor10",
            "sensor11", "sensor12", "sensor13", "sensor14", "sensor15",
            "sensor16", "sensor17", "sensor18", "sensor19", "sensor20",
            "sensor21"
        }).setOutputCol("features");
        aux = va.transform(ds);
        Normalizer fn = new Normalizer()
                .setInputCol("features") // non normalized feature
                .setOutputCol("nfeatures") // featured feature
                .setP(1.0);
        aux = fn.transform(aux);
        System.out.println("Transformed!");
        aux.show();
        return aux;
    }

    public static void extractFeatures() {

    }

    public static Dataset<?> removeNoise(Dataset<?> ds, ArrayList<PipelineModel> noiseModels) {
        int size = noiseModels.size();
        VectorAssembler va = new VectorAssembler().setInputCols(new String[]{"opm1", "opm2", "opm3", "cycle"}).setOutputCol("opm");
        Normalizer ln = new Normalizer()
                .setInputCol("opm")
                .setOutputCol("opms")
                .setP(1.0);
        Dataset<Row> inputs = va.transform(ds);
        inputs = ln.transform(inputs);
        Dataset<Row> tmp = inputs.select("*"); //  we create and empty dataframe

        Dataset<Row> diff = ds.select("*");
        for (int i = 1; i < size + 1; i++) {
            tmp = noiseModels.get(i - 1).transform(tmp);
            tmp = tmp.withColumn("sensor" + i, expr("prediction*1"));
            tmp = tmp.drop("prediction");
            tmp.show();

        }
        String[] headers = tmp.columns();
        ArrayList<String> sensorHeaders = new ArrayList();
        for (int i = 0; i < headers.length; i++) {
            System.out.println("H " + headers[i] + " BSBTR " + headers[i].substring(0, 3));
            if (headers[i].substring(0, 3).equalsIgnoreCase("sen")) {
                sensorHeaders.add(headers[i]);
            }

        }
        String header;
        diff = diff.select(col("unit").as("du"), col("sensor1").as("s1"), col("sensor2").as("s2"),
                col("sensor3").as("s3"), col("sensor4").as("s4"), col("sensor5").as("s5"), col("sensor6").as("s6"),
                col("sensor7").as("s7"), col("sensor8").as("s8"), col("sensor9").as("s9"), col("sensor10").as("s10"),
                col("sensor11").as("s11"), col("sensor12").as("s12"), col("sensor13").as("s13"), col("sensor14").as("s14"),
                col("sensor15").as("s15"), col("sensor16").as("s16"), col("sensor17").as("s17"), col("sensor18").as("s18"),
                col("sensor19").alias("s19"), col("sensor20").as("s20"), col("sensor21").as("s21"));

        diff.show(10);
        tmp.show(10);
        for (int i = 0; i < headers.length; i++) {
            System.out.println("H " + headers[i] + " BSBTR " + headers[i].substring(0, 3));
            if (headers[i].substring(0, 3).equalsIgnoreCase("sen")) {
                //System.out.println("989898"+headers[i]);
                sensorHeaders.add(headers[i]);
            }

        }

        size = headers.length;
        // we join 
        tmp = tmp.join(diff, tmp.col("unit").equalTo(diff.col("du")));
        System.out.println("After joint ");
        tmp.show(30);

        for (int i = 1; i < 22; i++) {
            // we substract all
            System.out.println("Sensor" + i);
            tmp = tmp.withColumn("sensor" + i, expr("sensor" + i + " - s" + i)).drop("s" + i);
            tmp.show(5);
        }
        tmp.show(10);

        // now clean the dataset by subscribing
        return tmp;
    }

    public static void main(String[] args) throws InterruptedException, MalformedURLException {
        // we distable the Spark Logger 
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        // we establish a database connection
        databaseMod dbm = new databaseMod("EngineUnits");
        // we import the saved model
        ArrayList<PipelineModel> sensorModels = new ArrayList();
        // we read all the sensor models from the specified directory
        String basePath = "C:\\mygrad\\proPhm\\Sjob0\\sensorModel\\s";
        
        
        // we connect to the mqtt broker in the address cookbae.com:1883
        String brokerUrl = "tcp://cookbae.com:1883";
        String topics = "eunit";
        // we create spark context
        SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("Real Time Analyser");

        //JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.hadoopConfiguration().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        // we create the spark sql context7
        // we create the streamming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Minutes.apply(1));

        SparkSession spark = SparkSession.builder().master("local[5]").getOrCreate();
        // we create an encoder to describe our data

        Encoder<MUnit> MUnitEncoder = Encoders.bean(MUnit.class);
        for (int i = 1; i < 22; i++) {
            sensorModels.add(PipelineModel.read().load(basePath + i));
        }
        PipelineModel pm = PipelineModel.read().load("C:\\mygrad\\proPhm\\Sjob0\\Models\\dtr");
        // we checkpoint the result the linux file system
        jssc.checkpoint("/home/pentanol/stremerlog");
        JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topics);
        messages.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> sdata) throws Exception {
                //System.out.println("It was called!");
                Dataset<?> aux;
                long size = sdata.count();
                ArrayList<MUnit> UList = new ArrayList();
                Dataset<MUnit> ds = null;
                // ds.re
                JavaRDD<String> fsdata = null; // a formatted sensor data RDD
                //System.out.println(" == >" + size);
                if (size >= 0) {
                    // do your work here

                    // feed data to prediction model
                    // save predicted result
                    //spark.createDataFrame(sdata, String);
                    // create the dataset
                    for (String line : sdata.collect()) {
                        // tranform to numerical form
                        System.out.println(line);
                        UList.add(new MUnit(line));
                        //System.out.println("Record Created!!!!!!");

                    }
                    ds = spark.createDataset(UList, MUnitEncoder);
                    ds.show(10);
                    // clean sesor data drom noise 
                    // clean noise
                    aux = removeNoise(ds, sensorModels);
                    // format data
                    aux = mormalizeAndSelect(aux);
                    aux.show();
                    System.out.println("data formatted");
                    // feed the result to the exsisting model
                    aux = pm.transform(aux);                    
                    aux =  aux.select("unit","cycle","prediction");
                    aux.show();
                   
                    //System.out.println("Print this works" + topics);
                }
                
                // feed to model for analyse
                // persist result to database
                //System.out.println("* " + line);

            }
        }
        );

        System.out.println("We are here with you!");
        jssc.start();

        jssc.awaitTermination();

    }

}
