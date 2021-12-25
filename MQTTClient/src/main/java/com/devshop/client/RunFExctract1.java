/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;

import org.apache.spark.ml.evaluation.RegressionEvaluator;

import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RunFExctract1 {

    public static void main(String[] args) throws FileNotFoundException, IOException {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Engine Modelizer")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Normalizer fn = new Normalizer()
                .setInputCol("features")
                .setOutputCol("nfeatures")
                .setP(1.0);
        Normalizer ln = new Normalizer()
                .setInputCol("label")
                .setOutputCol("nlabel")
                .setP(1.0);
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

        RFormula formula = new RFormula()
                .setFormula("rul ~ sensor1 + sensor2 + sensor3 + sensor4 + sensor5 + sensor6 + sensor7 + sensor8 + sensor9 + sensor10 + sensor11 + sensor12 + sensor13 + sensor14 + sensor15 + sensor16 + sensor17 + sensor18 + sensor19 + sensor20 + sensor21")
                .setFeaturesCol("features")
                .setLabelCol("label");
        Dataset<Row> ts = formula.fit(trainingSet).transform(trainingSet).select("features", "label");
        ts.show();
        ts = fn.transform(ts);
        ts = ln.transform(ts);
        ts = ts.select("nfeatures", "label");
        ts.show();
        Dataset<Row> tset = formula.fit(testSet).transform(testSet).select("features", "label");
        tset = fn.transform(tset);
        tset = ln.transform(tset);
        tset = ts.select("nfeatures", "label");

        //Dataset<Row>[] splits = ts.randomSplit(new double[]{0.6, 0.4});
        DecisionTreeRegressor dt = new DecisionTreeRegressor().setMaxDepth(30).setFeaturesCol("nfeatures");
        RandomForestRegressor rf = new RandomForestRegressor().setMaxDepth(30).setFeaturesCol("nfeatures");
        GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
                .setFamily("gaussian")
                .setLink("identity")
                .setMaxIter(10)
                .setRegParam(0.3);
        GBTRegressor gbt = new GBTRegressor().setLabelCol("label")
                .setFeaturesCol("nfeatures")
                .setMaxIter(220);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{rf});
        PipelineModel model = pipeline.fit(ts);
        Dataset<Row> predictions = model.transform(tset);
        predictions.select("label", "nfeatures").show(20);
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
               
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    }

}
