/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.sjob0;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class RunFExctract {
    // create a spark context
    public static SparkSession ss;
    // get rid of unecessary dimenstions
    public static Dataset<Row> reduceDimension(Dataset<Row> ds, String inputCol, String outputCol, int k) {
        Dataset<Row> rds;
        PCAModel pca = new PCA()
                .setInputCol(inputCol) // the input vector label
                .setOutputCol(outputCol) // the output vector label
                .setK(k) // the desired number of dimensions
                .fit(ds); // call for to reduce the number
        rds = pca.transform(ds).select(outputCol);
        return rds;
    }
    // read dataset as text and split it
    public static ArrayList<Dataset<Row>> readDatasets(String[] paths, StructType[] schema) {
        ArrayList<Dataset<Row>> ds = new ArrayList();
        int size = paths.length;
        Dataset<Row> tmp;
        for (int i = 0; i < size; i++) {

            tmp = ss.read().option("delimiter", " ").schema(schema[i]).csv(paths[i]);
            ds.add(tmp);
        }
        return ds;
    }
    // create a pipeline model for raw sensor data / with noise
    public static PipelineModel rawSensorModel(Dataset<Row> sData, Pipeline pl) {
        Dataset<Row> tmp;
        PipelineModel plm;
        RFormula formula = new RFormula()
                .setFormula("rul ~ sensor1 + sensor2 + sensor3 + sensor4 + sensor5 + sensor6 + sensor7 + sensor8 + sensor9 + sensor10 + sensor11 + sensor12 + sensor13 + sensor14 + sensor15 + sensor16 + sensor17 + sensor18 + sensor19 + sensor20 + sensor21")
                .setFeaturesCol("nfeatures")
                .setLabelCol("label");
        tmp = formula.fit(sData).transform(sData).select("nfeatures", "label");
        plm = pl.fit(sData);
        return plm;     
    }
    // model the noise in order to remove it
    public static ArrayList<PipelineModel> noiseModel(Dataset<Row> ds, Pipeline pl) throws IOException {
        RFormula formula = new RFormula();
        Dataset<Row> tmp, aux;
        PipelineModel tmpModel;
        ArrayList<PipelineModel> pln = new ArrayList<PipelineModel>();
        String[] sensorLabels = {"sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7",
            "sensor8", "sensor9", "sensor10", "sensor11", "sensor12", "sensor13", "sensor14", "sensor15", "sensor16",
            "sensor17", "sensor18", "sensor19", "sensor20", "sensor21"};
        VectorAssembler va = new VectorAssembler().setInputCols(new String[]{"opm1", "opm2", "opm3", "cycle"}).setOutputCol("opm");
        aux = va.transform(ds);
        Normalizer ln = new Normalizer()
                .setInputCol("opm")
                .setOutputCol("opmi")
                .setP(1.0);
        aux = ln.transform(aux);

        for (int i = 0; i < sensorLabels.length; i++) {
            formula.setFormula(sensorLabels[i] + "~ opmi ").setFeaturesCol("opms").setLabelCol("label");
            tmp = formula.fit(aux).transform(aux.drop("unit")).select("opms", "label");
            tmpModel = pl.fit(tmp);
            tmpModel.save("sensorModels\\s" + (i + 1));
            pln.add(tmpModel);
        }

        return pln;
    }
    // remove noise from data
    public static Dataset<Row> removeNoiseEffect(ArrayList<PipelineModel> noiseModels, Dataset<Row> ds) {

        int size = noiseModels.size();
        VectorAssembler va = new VectorAssembler().setInputCols(new String[]{"opm1", "opm2", "opm3", "cycle"}).setOutputCol("opm");
        Normalizer ln = new Normalizer()
                .setInputCol("opm")
                .setOutputCol("opms")
                .setP(1.0);
        Dataset<Row> tmp = va.transform(ds);
        tmp = ln.transform(tmp);
        //Dataset<Row> tmp ; //  we create and empty dataframe
        Dataset<Row> diff = ds.select("*");
        //tmp = inputs.select("*");
        //tmp.show();

        for (int i = 1; i < size + 1; i++) {
            tmp = noiseModels.get(i - 1).transform(tmp);
            tmp = tmp.withColumn("sensor" + i, expr("prediction*1"));

            tmp = tmp.drop("prediction");
        }
        String[] headers = tmp.columns();
        ArrayList<String> sensorHeaders = new ArrayList();
        for (String header : headers) {
            if (header.substring(0, 3).equalsIgnoreCase("sen")) {
                sensorHeaders.add(header);
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
            //System.out.println("H " + headers[i] + " BSBTR " + headers[i].substring(0, 3));
            if (headers[i].substring(0, 3).equalsIgnoreCase("sen")) {
                //System.out.println("989898"+headers[i]);
                sensorHeaders.add(headers[i]);
            }

        }
        size = headers.length;
        // we join 
        tmp = tmp.join(diff, tmp.col("unit").equalTo(diff.col("du")));
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
    // makes no sense
    public static Dataset<Row> eliminateNoise(Dataset<Row> ds) {
        return null;
    }
   // makes no sense
    public static void modelWorkModel(Dataset<Row> ds) throws IOException {

    }
// main executin here
    public static void main(String[] args) throws FileNotFoundException, IOException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        /*
        Context initialization section
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Engine RUL Modelizer")
                .config("spark.sql.caseSensitive", "false")
                .getOrCreate();
        ss = spark;
        String[] paths = {
            "dataset\\train_FD002.txt",
            "dataset\\test_FD002.txt"
        };
        Normalizer fn = new Normalizer()
                .setInputCol("features")
                .setOutputCol("nfeatures")
                .setP(1.0);
        Normalizer ln = new Normalizer()
                .setInputCol("label")
                .setOutputCol("nlabel")
                .setP(1.0);
        StructType schema = new StructType(new StructField[]{
            new StructField("unit", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("cycle", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("opm1", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("opm2", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("opm3", DataTypes.DoubleType, false, Metadata.empty()),
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

        ArrayList<Dataset<Row>> dsts = readDatasets(paths, new StructType[]{schema, schema});
        /*
        Training set processing
         */
        Dataset< Row> trainingSet = dsts.get(0);

        Dataset<Row> nums = trainingSet.groupBy(col("unit")).count().select(col("unit").as("ca"), col("count"));

        // we modify the training set
        Dataset<Row> inter = trainingSet.join(nums, trainingSet.col("unit").equalTo(nums.col("ca"))).drop("ca");

        Dataset<Row> ts = inter.withColumn("rul", expr("count - cycle"));
        //System.out.println("/*//*/*/*/"+ts.columns()[i].substring(0, 2) );
        /*
        Test set processing
         */
        Dataset<Row> testSet = dsts.get(1);
        nums = testSet.groupBy(col("unit")).count().select(col("unit").as("ca"), col("count"));
        //nums.show();

        inter = testSet.join(nums, testSet.col("unit").equalTo(nums.col("ca"))).drop("ca");
        //inter = testSet.join(nums,"unit").drop("tss");

        //inter.join(ts, joinExprs)
        Dataset<Row> tset = inter.withColumn("rul", expr("count - cycle"));
        ts.show();
        tset.show();

        /*
        Feature selection and normalizing
         */
        // we initialize the Rformula to train and test our model "formula"
        RFormula formula = new RFormula()
                .setFormula("rul ~ sensor1 + sensor2 + sensor3 + sensor4 + sensor5 + sensor6 + sensor7 + sensor8 + sensor9 + sensor10 + sensor11 + sensor12 + sensor13 + sensor14 + sensor15 + sensor16 + sensor17 + sensor18 + sensor19 + sensor20 + sensor21")
                .setFeaturesCol("features")
                .setLabelCol("label");

        /*
        Modeling section 
         */
        // we model our sensor noise due to working condtions we use the decision tree regressor
        DecisionTreeRegressor noiseModel = new DecisionTreeRegressor().setMaxDepth(15).setFeaturesCol("opms");
        Pipeline noisePipe = new Pipeline().setStages(new PipelineStage[]{noiseModel});
        ArrayList<PipelineModel> noisePipelineModelsTrain = noiseModel(ts, noisePipe); // all sensors noise models calculated
        ts = removeNoiseEffect(noisePipelineModelsTrain, ts);

        //ArrayList<PipelineModel> noisePipelineModelsTest = noiseModel(tset, noisePipe); // all sensors noise models calculated
        //tset = removeNoiseEffect(noisePipelineModelsTrain, tset);
        // we calculate the raw data model with noise
        // for the dataset
        // for the training set
        // we normalise the new sensor data 
        // we normalize the training set
        ts = formula.fit(ts).transform(ts).select("features", "label");
        ts.show();

        ts = fn.transform(ts);
        ts = ln.transform(ts);
        ts = ts.select("nfeatures", "label");

        tset = formula.fit(tset).transform(tset).select("features", "label");
        // we normalize the test set
        tset = fn.transform(tset);
        tset = ln.transform(tset);
        tset = tset.select("nfeatures", "label");

        // Machine Learning Session : Modeling Part

        DecisionTreeRegressor dt = new DecisionTreeRegressor().setMaxDepth(20).setFeaturesCol("nfeatures");
        RandomForestRegressor rf = new RandomForestRegressor().setMaxDepth(20).setFeaturesCol("nfeatures");
        GBTRegressor gbt = new GBTRegressor().setLabelCol("label")
                .setFeaturesCol("nfeatures")
                .setMaxIter(200);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{dt});
        ts.show();
        PipelineModel model = pipeline.fit(ts);
        //model.save("Models\\dtr");
        Dataset<Row> predictions = model.transform(tset);
        predictions.select("label", "nfeatures").show(20);
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        /*
        We evaluate and save our model
         */
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    }

}
