/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FeatureExtract {

    private FileParser fp;
    private Dataset df;
    private final CSVReader sr;
    private JavaSparkContext jsc;
    private List<double[]> dsd;

    FeatureExtract(String filename) throws FileNotFoundException, IOException {
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(' ')
                .withIgnoreQuotations(true)
                .build();

        sr = new CSVReaderBuilder(new FileReader(filename))
                .withSkipLines(0)
                .withCSVParser(parser)
                .build();
        this.dsd = this.datasetToInt();
        //this.vectorizeDataset();
        /*
        SparkSession spark = SparkSession.builder().master("local")
                .appName("Word Count")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset ds = spark.read().csv(filename);
        ds.show();
         */
    }

    //public void vectorizeDataset() throws IOException {
    //    this.calculateRUL(this.datasetToInt());
    //}
    private List<Row> vectorizeRow() {

        return null;
    }

    //private static int calculateRUL(List<double[]> dataset) {
    private int calculateRUL(List<double[]> dataset) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Feature Extractor");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<double[]> rdd = sc.parallelize(dataset);
        JavaPairRDD<Integer, Integer> xc = rdd.mapToPair((double[] t) -> new Tuple2((int) t[0], 1));//.reduceByKey((Integer t1, Integer t2) -> t1+t2 //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        JavaPairRDD<Integer, Integer> counts = (JavaPairRDD<Integer, Integer>) xc.countByKey();
        JavaPairRDD<Integer, double[]> rddUnit = rdd.mapToPair((double[] t) -> new Tuple2((int) t[0], t));
        //JavaPairRDD<Integer, Tuple2<double[],Integer>> lom = rddUnit.groupByKey().mapToPair((Iterable t) -> t.);
        JavaPairRDD<Integer, Iterable<double[]>> dl = rddUnit.groupByKey();
        //JavaPairRDD<Integer, Integer> idCount = rddUnit.reduceByKey(());
        rddUnit.saveAsTextFile("C:\\mygrad\\op.txt\\001");
        dl.saveAsTextFile("C:\\mygrad\\op.txt\\002");
        xc.saveAsTextFile("C:\\mygrad\\op.txt\\003");
        System.out.println(rddUnit.take(2).toArray());
        return 125;
    }

    private int[] undLine(String line) {
        String[] tmp = line.split(" ");
        int[] casted = new int[tmp.length];
        for (int i = 0; i < tmp.length; i++) {
            casted[i] = Integer.parseInt(tmp[i]);

        }
        return casted;
    }

    private List<double[]> datasetToInt() throws IOException {
        List<String[]> list = new ArrayList<>();
        List<double[]> dataset = new ArrayList<>();
        list = sr.readAll();
        int size = list.size();
        for (int i = 0; i < size; i++) {
            double[] tmp = new double[list.get(i).length];
            for (int j = 0; j < list.get(i).length - 2; j++) {
                System.out.print("" + list.get(i)[j] + " ");

                tmp[j] = Double.parseDouble(list.get(i)[j]);
            }
            System.out.println();
            dataset.add(tmp);
        }
        sr.close();
        return dataset;
    }

    public List<double[]> getDataset() {
        return this.dsd;
    }

}
