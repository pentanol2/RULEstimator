package com.devshop.sjob0;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//import static com.devshop.sjob0.Collector.colsAsObjects;


/**
 *
 * @author DevShop
 */
public class Runner_bcp {

    public static SparkSession ss;

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // path to training sets
        String[] train_dataset_paths = {
            "dataset\\train_FD001.txt",
            "dataset\\train_FD002.txt",
            "dataset\\train_FD003.txt",
            "dataset\\train_FD004.txt",};

        // path to test sets
        String[] test_dataset_paths = {
            "dataset\\test_FD001.txt",
            "dataset\\test_FD002.txt",
            "dataset\\test_FD003.txt",
            "dataset\\test_FD004.txt"};

        // path to test set labels 
        String[] y_test_dataset_paths = {
            "dataset\\RUL_FD001.txt",
            "dataset\\RUL_FD002.txt",
            "dataset\\RUL_FD003.txt",
            "dataset\\RUL_FD004.txt"};

        // initilize spark context
        ss = SparkSession.builder()
                .master("local")
                .appName("RUL Estimator")
                .config("spark.sql.caseSensitive", "false")
                .getOrCreate();

        // set dataset schema for both training and test
        StructType trainSchema = new StructType(new StructField[]{
            new StructField("unit_no", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("cycle", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("param_1", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("param_2", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("param_3", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_1", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_2", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_3", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_4", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_5", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_6", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_7", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_8", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_9", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_10", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_11", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_12", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_13", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_14", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_15", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_16", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_17", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_18", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_19", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_20", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("sensor_21", DataTypes.DoubleType, false, Metadata.empty())
        });

        // dataset schema for test label files
        StructType labelSchema = new StructType(new StructField[]{
            new StructField("RUL", DataTypes.IntegerType, false, Metadata.empty())
        });

        Column[] index_cols = Collector.colsAsObjects(new String[]{"unit_no"});
        Column[] parmater_cols = Collector.colsAsObjects(new String[]{"param_1","param_2","param_3"});
        Column[] sensor_cols = Collector.colsAsObjects(new String[]{"sensor_1","sensor_2","sensor_3","sensor_4","sensor_5","sensor_6","sensor_7","sensor_8","sensor_9",
                "sensor_10","sensor_11","sensor_12","sensor_13","sensor_14","sensor_15","sensor_16","sensor_17","sensor_18","sensor_19",
                "sensor_20","sensor_21"});


        Collector train_sets = new Collector(ss, train_dataset_paths, trainSchema);
        Collector test_sets = new Collector(ss, test_dataset_paths, trainSchema); // in our case train and test have the same schema
        Collector label_sets = new Collector(ss, y_test_dataset_paths, labelSchema);


        Dataset<Row> ts = train_sets.getDataset(0);
        // System.out.println(ts.groupBy(col("unit_no")).count());
        //ts.groupBy(col("unit_no")).count().select(col("unit_no").as("ca"), col("count")).sort("count").show();
/*
        Dataset<Row> mins = ts.groupBy(col("unit_no")).min(ts.columns());
        mins.show();
        Dataset<Row> max = ts.groupBy(col("unit_no")).max(ts.columns());
        max.show();
        Dataset<Row> mean = ts.groupBy(col("unit_no")).avg(ts.columns());
        mean.show();
         */

        /// System.out.println(ts.get(0));
        String[] cols = ts.columns();
        StructType summarySchema = new StructType(new StructField[]{
                new StructField("feature_name",DataTypes.StringType,false,Metadata.empty()),
                new StructField("max", DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("min",DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("std",DataTypes.DoubleType,false, Metadata.empty())
        });
        // EDA --> Rets the rows to get rid of as they are non effective
        ExploratoryDataAnalysis eda = new ExploratoryDataAnalysis(ts);
        eda.check_invariant_features(0.02);

    }
}