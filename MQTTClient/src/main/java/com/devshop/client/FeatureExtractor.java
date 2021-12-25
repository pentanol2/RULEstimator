/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author DevShop
 */
public class FeatureExtractor {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Feature Extractor")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        StructType schema = new StructType(new StructField[]{
            new StructField("id", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("cycle", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("opcon1", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("opcon2", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("opcon3", DataTypes.DoubleType, false, Metadata.empty()),
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
        Dataset<Row> trainingSet = spark.read().schema(schema).option("header", true).option("delimiter", " ").csv("C:\\mygrad\\python-ml-turbofan-master\\CMAPSSData\\train_FD001.txt");
        VectorAssembler sensors = new VectorAssembler()
                .setInputCols(new String[]{"sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7", "sensor8", "sensor9", "sensor10", "sensor11", "sensor12", "sensor13", "sensor14", "sensor15", "sensor16", "sensor17", "sensor18", "sensor19", "sensor20", "sensor21"})
                .setOutputCol("sensors");
        VectorAssembler opcons = new VectorAssembler()
                .setInputCols(new String[]{"opcon1", "opcon2", "opcon3"})
                .setOutputCol("opcons");
        RFormula vec2vec = new RFormula()
                .setFormula("sensors ~ opcons")
                .setFeaturesCol("oVec")
                .setLabelCol("sVec");

        RFormula formula2 = new RFormula()
                .setFormula("sensor1 + sensor2 + sensor3 + sensor4 + sensor5 + sensor6 + sensor7 + sensor8 + sensor9 + sensor10 + sensor11 + sensor12 + sensor13 + sensor14 + sensor15 + sensor16 + sensor17 + sensor18 + sensor19 + sensor20 + sensor21 ~ opcon1 + opcon2 + opcon3 ")
                .setFeaturesCol("features")
                .setLabelCol("label");
        Dataset<Row> op2sen = opcons.transform(sensors.transform(trainingSet));
        op2sen.show();
        op2sen = op2sen.select("opcons", "sensors");
        //op2sen = (new VectorSlicer().setInputCol("opcons").setOutputCol("sensors").setIndices(new int[]{0,1})).transform(op2sen);
        op2sen.show();

        PCAModel pca = new PCA()
                .setInputCol("sensors")
                .setOutputCol("sens")
                .setK(1)
                .fit(op2sen);
        Dataset<Row> tmp = pca.transform(op2sen);
        tmp.show();
        GBTRegressor gbt = new GBTRegressor()
                .setLabelCol("sensors")
                .setFeaturesCol("opcons")
                .setMaxIter(10);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{gbt});
        PipelineModel model = pipeline.fit(op2sen);
        Dataset<Row> predictions = model.transform(op2sen);
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    }

}
