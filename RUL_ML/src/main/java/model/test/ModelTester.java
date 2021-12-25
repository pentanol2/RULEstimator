package model.test;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;

public class ModelTester {
    private PipelineModel testModel = null;
    private Dataset<Row> testSet = null;
    private Dataset<Row> yTest = null;
    private RegressionEvaluator evaluator = new RegressionEvaluator();

    public ModelTester(PipelineModel testModel, Dataset<Row> testSet,Dataset<Row> yTest){
        this.testModel = testModel;
        this.testSet = testSet;
        this.yTest = yTest;
    }

    public ModelTester(String modelPath,Dataset<Row> testSet, Dataset<Row> yTest){
        File f = new File(modelPath);
        if (!f.exists()){
            System.out.println("Model path invalid!");
            System.exit(1);
        }
        this.testSet = testSet;
        this.testModel = PipelineModel.load(modelPath);
        System.out.println("Model loaded "+this.testModel.uid());
    }

    public void setTestModel(PipelineModel model){
        this.testModel = model;
    }

    public Dataset<Row> testModel(){

        Dataset<Row> predTestResult = this.testModel.transform(this.testSet);
        evaluator.setLabelCol("RUL")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        System.out.println("RMSE evaluation result : "+evaluator.evaluate(predTestResult));
        return predTestResult;
    }
}
