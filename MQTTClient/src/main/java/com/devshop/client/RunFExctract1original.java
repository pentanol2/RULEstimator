/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static org.apache.spark.api.java.StorageLevels.DISK_ONLY;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.regression.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Seq;

public class RunFExctract1original {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        /*
        List<double[]> dataset;
        FeatureExtract fe = new FeatureExtract("C:\\mygrad\\python-ml-turbofan-master\\CMAPSSData\\test_FD001.txt");
        dataset = fe.getDataset();
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("Feature Extractor");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<double[]> rdd = sc.parallelize(dataset);
         */
 /*
        SparkSession spark = SparkSession.builder().master("local")
                .appName("Engine Modelizer")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
         */
        //rdd.checkpoint();
        /*
        JavaPairRDD<Integer, Integer> xc = rdd.mapToPair((double[] t) -> new Tuple2((int) t[0], 1));//.reduceByKey((Integer t1, Integer t2) -> t1+t2 //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        JavaPairRDD<Integer, Integer> counts = xc.reduceByKey((Integer a, Integer b) -> a + b).sortByKey();
        JavaPairRDD<Integer, double[]> rddUnit = rdd.mapToPair((double[] t) -> new Tuple2((int) t[0], t));
        JavaPairRDD<Integer, Tuple2<double[], Integer>> cxc = rddUnit.join(counts).sortByKey();
        JavaPairRDD<Integer, Tuple2<double[], Double>> ctx = cxc.mapValues((Tuple2<double[], Integer> t) -> {
            return new Tuple2<double[], Double>(t._1(), (t._2 - t._1[1]));
        });
         */
 /*
        .foreach((Tuple2<Integer, Tuple2<double[], Integer>> t) -> {
            Tuple2<double[], Integer> tup = new Tuple2<double[], Integer>(t._2._1, ((t._2._2) - (t._2._1[1])));
            t = new Tuple2<Integer, Tuple2<double[], Integer>>(t._1, tup);
        });
         */
        //cxc.for

        //JavaPairRDD<Integer, Iterable<double[]>> dl = rddUnit.groupByKey();
        /*
        JavaRDD<String> lines = ctx.map(new Function<Tuple2<Integer, Tuple2<double[], Double>>, String>() {

            public String call(Tuple2<Integer, Tuple2<double[], Double>> t) throws Exception {
                String line = "";
                int size = 0;
                //line = t._1 + " ";
                double[] vect = t._2._1;
                size = vect.length - 2;
                for (int i = 5; i < size; i++) {
                    line += vect[i] + " ";
                }
                line += t._2._2;
                //System.out.println(line);
                return line;

            }
        });
         */
        //JavaPairRDD<Integer, Integer> idCount = rddUnit.reduceByKey(());
        //rddUnit.saveAsTextFile("C:\\mygrad\\op.txt\\001");
        //dl.saveAsTextFile("C:\\mygrad\\op.txt\\002");
        //cxc.saveAsTextFile("C:\\mygrad\\text.op\\cxc");
        //ctx.saveAsTextFile("C:\\mygrad\\text.p\\xtc");
        /*
        lines.saveAsTextFile("C:\\mygrad\\text.p\\test");
        sc.close();
         System.exit(0);
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Engine Modelizer")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
            new StructField("sensor1", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor2", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor3", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor4", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor5", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor6", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor7", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor8", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor9", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor10", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor11", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor12", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor13", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor14", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor15", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor16", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor17", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor18", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor19", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor20", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor21", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("rul", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> trainingSet = spark.read().schema(schema).option("header", true).option("delimiter", " ").csv("C:\\mygrad\\text.p\\csv\\");
        Dataset<Row> testSet = spark.read().schema(schema).option("header", true).option("delimiter", " ").csv("C:\\mygrad\\text.p\\test");
        //Dataset<Row> df = spark.createDataFrame(drdd, schema);
        // we configure feature selectors 
        /*
        Attribute[] attrs = new Attribute[]{
            NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(1),
            NumericAttribute.defaultAttr().withIndex(2),
            NumericAttribute.defaultAttr().withIndex(3),
            NumericAttribute.defaultAttr().withIndex(4),
            NumericAttribute.defaultAttr().withIndex(5),
            NumericAttribute.defaultAttr().withIndex(6),
              NumericAttribute.defaultAttr().withIndex(7),
            NumericAttribute.defaultAttr().withIndex(9),
            NumericAttribute.defaultAttr().withIndex(10),
            NumericAttribute.defaultAttr().withIndex(11),
            NumericAttribute.defaultAttr().withIndex(12),
            NumericAttribute.defaultAttr().withIndex(13),
            NumericAttribute.defaultAttr().withIndex(14),
              NumericAttribute.defaultAttr().withIndex(15),
            NumericAttribute.defaultAttr().withIndex(16),
            NumericAttribute.defaultAttr().withIndex(17),
            NumericAttribute.defaultAttr().withIndex(18),
            NumericAttribute.defaultAttr().withIndex(19),
            NumericAttribute.defaultAttr().withIndex(20),
            /*
              NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(0),
            NumericAttribute.defaultAttr().withIndex(0),
            
        };
         */
 /*
        AttributeGroup group = new AttributeGroup("sensors", attrs);
        VectorSlicer vectorSlicer = new VectorSlicer().setInputCol("sensors").setOutputCol("url");
        Dataset<Row> dss = spark.read().schema((new StructType()).add(group.toStructField())).csv("C:\\mygrad\\text.p\\csv\\");
        dss.show();
        vectorSlicer.setIndices(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
        DecisionTreeRegressor dt = new DecisionTreeRegressor();
        Dataset<Row> op = vectorSlicer.transform(trainingSet);
         */
        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("nfeatures")
                .setP(1.0);
        RFormula formula = new RFormula()
                .setFormula("rul ~ sensor1 + sensor2 + sensor3 + sensor4 + sensor5 + sensor6 + sensor7 + sensor8 + sensor9 + sensor10 + sensor11 + sensor12 + sensor13 + sensor14 + sensor15 + sensor16 + sensor17 + sensor18 + sensor19 + sensor20 + sensor21")
                .setFeaturesCol("features")
                .setLabelCol("label");
        Dataset<Row> ts = formula.fit(trainingSet).transform(trainingSet).select("features", "label");
        ts.show();
        ts = normalizer.transform(ts);
        ts.show();
        Dataset<Row> tset = formula.fit(testSet).transform(testSet).select("features", "label");
        tset.show();
        tset = normalizer.transform(tset);
        tset.show();
        //Dataset<Row>[] splits = ts.randomSplit(new double[]{0.6, 0.4});
        DecisionTreeRegressor dt = new DecisionTreeRegressor().setFeaturesCol("features");
        RandomForestRegressor rf = new RandomForestRegressor().setFeaturesCol("features");
        GBTRegressor gbt = new GBTRegressor().setLabelCol("label")
                .setFeaturesCol("nfeatures")
                .setMaxIter(200);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{dt});
        PipelineModel model = pipeline.fit(ts);
        //model.save("C:\\mygrad\\text.p\\csv\\model");
        Dataset<Row> predictions = model.transform(tset);
        predictions.show();
        predictions.select("label", "features").show(20);
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

        System.out.println("------------------>" + ts.count());;
    }

}
