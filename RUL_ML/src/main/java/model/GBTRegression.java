package model;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class GBTRegression extends Model{

    private GBTRegressor alg = null;
    private GBTRegressionModel model = null;
    private Dataset<Row> ds = null;
    public GBTRegression(String modelPath){
        //this.model = GBTRegressionModel.read(modelPath);

    }
    public GBTRegression(Dataset<Row> ds, String featureCols, String labelCol, int maxDepth){
        this.ds = ds;
        alg = new GBTRegressor()
                .setMaxDepth(maxDepth)
                .setFeaturesCol(featureCols)
                .setLabelCol(labelCol);
    }

    public void train(){
        this.pModel = new Pipeline().setStages(new PipelineStage[]{this.alg}).fit(this.ds);
    }

    public void save(String path) throws IOException {
        this.pModel.save(path);
    }

    @Override
    public Dataset<Row> predict(Dataset<Row> testSet) {
        return pModel.transform(testSet);
    }

}
