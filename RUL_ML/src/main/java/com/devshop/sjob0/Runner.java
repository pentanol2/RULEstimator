package com.devshop.sjob0;

import model.test.ModelTester;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Column;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import model.Model;
import model.RegressionTree;
import model.RandomForestRegression;
import model.GBTRegression;
/**
 *
 * @author DevShop
 */
public class Runner {

    public static SparkSession ss;

    public static void main(String[] args) {
        Boolean trainingDone = null;
        Scanner scanner = new Scanner(System.in);
        System.out.print("Do you like to run on training mode (y/n) : ");
        String ip = scanner.nextLine().toLowerCase();
        if(ip.equals("y")){
            trainingDone = false;
        }else if(ip.equals("n")){
            trainingDone = true;
        }else{
            System.out.println("Wrong input!");
            return;
        }


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

        Collector trainSets = new Collector(ss, train_dataset_paths, trainSchema);
        Collector testSets = new Collector(ss, test_dataset_paths, trainSchema); // in our case train and test have the same schema
        Collector labelSets = new Collector(ss, y_test_dataset_paths, labelSchema);

        Dataset<Row> trainSet1 = trainSets.getDataset(0);
        Dataset<Row> testSet1 = testSets.getDataset(0);
        /// System.out.println(trainSet1.get(0));
        String[] cols = trainSet1.columns();
        StructType summarySchema = new StructType(new StructField[]{
                new StructField("feature_name",DataTypes.StringType,false,Metadata.empty()),
                new StructField("max", DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("min",DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("std",DataTypes.DoubleType,false, Metadata.empty())
        });
        // EDA --> Rets the rows to get rid of as they are non effective
        ExploratoryDataAnalysis eda = new ExploratoryDataAnalysis(trainSet1);
        ArrayList<String> toDrop = eda.check_invariant_features(0.02);

        // Drop Columns with low variance
        // drop unnecessary cols from train set
        Dataset<Row> traiDsColsDropped = Preprocess.drop_colums(trainSet1,toDrop.toArray(new String[toDrop.size()]));
        // calculate the rul for the training set
        trainSet1 = Preprocess.calcRUL(traiDsColsDropped);
        // drop indices and temp columns from training set
        trainSet1 = trainSet1.drop(new String[]{"unit_no","cycle","max_rul"});
        // Get the normalization model for the training set. Will be applied later for the test set
        PipelineModel mmsm = Preprocess.makeMinMaxModel(trainSet1,"rul");
        // Normalize the training set
        trainSet1 = Preprocess.normalizeByColumnVect(trainSet1,mmsm,trainSet1.columns(),"rul");

        // setup and train different models
        Model[] models = new Model[]{
                /*
                new Pipeline().setStages(new PipelineStage[]{new RegressionTree(trainSet1,"norm_vect","rul",20)}),
                new Pipeline().setStages(new PipelineStage[]{new RandomForestRegression(trainSet1,"norm_vect","rul",20)}),
                new Pipeline().setStages(new PipelineStage[]{new GBTRegression(trainSet1,"norm_vect","rul",20)})
                */
                new RegressionTree(trainSet1,"norm_vect","rul",20),
                new RandomForestRegression(trainSet1,"norm_vect","rul",20),
                new GBTRegression(trainSet1,"norm_vect","rul",20)
        };

        // perform the training process for different models
        if (!trainingDone) {
            for (Model m : models) {
                m.train();
                try {
                    m.save("rul_models\\as_pipelines\\model" + m.toString());
                    System.out.println("Saved!");
                } catch (IOException e) {
                    System.out.println("Wrong path!");
                }
            }
            // in case the job is configured for training halt it. No test performed for this session
            return;
        }

        /*
        * Test Section: In this section we will perform model testing
        * */

        // set model path
        String modelPath = "rul_models\\as_pipelines";
        String[] pathNames = null;
        File f = new File(modelPath);
        pathNames = f.list();
        System.out.println("Choose the model you prefer perfom test on:");

        for (int i=0; i<pathNames.length;i++) {
            System.out.println("["+i+"] "+pathNames[i]);
        }
        ip = scanner.next();
        modelPath = modelPath+"\\"+pathNames[Integer.parseInt(ip)];
        System.out.println(modelPath);
        // process test set
        Dataset<Row> maxCycleRecs = testSet1.groupBy("unit_no").agg(max("cycle").as("max_cycle"));
        testSet1 = testSet1
                .join(maxCycleRecs,testSet1.col("unit_no").equalTo(maxCycleRecs.col("unit_no")))
                .filter("cycle = max_cycle")
                .drop(new String[]{"max_cycle","unit_no","cycle"});
        // drop unnecessary cols from test set
        // drop unnecessary cols from test set: former var name : testDsColsDropped
        testSet1 = Preprocess.drop_colums(testSet1,toDrop.toArray(new String[toDrop.size()]));
        testSet1 = Preprocess.normalizeByColumnVect(testSet1,mmsm,testSet1.columns(),"rul");
        testSet1 = testSet1.withColumn("id",monotonically_increasing_id());

        Dataset<Row> labelSet1 = labelSets.getDataset(0).withColumn("id",monotonically_increasing_id());
        testSet1 = labelSet1.join(testSet1,testSet1.col("id").equalTo(labelSet1.col("id"))).drop("id");

        // create model tester and test
        ModelTester predictor = new ModelTester(modelPath,testSet1,labelSets.getDataset(0));
        System.out.println("Test Set Predictions : ");
        predictor.testModel().show();
        System.exit(1);

    }
}