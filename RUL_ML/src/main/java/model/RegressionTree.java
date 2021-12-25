package model;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class RegressionTree extends Model{

    private DecisionTreeRegressor alg = null;
    private DecisionTreeRegressionModel model = null;
    private Dataset<Row> ds = null;

    public RegressionTree(String modelPath){

    }

    public RegressionTree(Dataset<Row> ds, String featureCols, String labelCol, int maxDepth){
        this.ds = ds;
        this.alg = new DecisionTreeRegressor()
                .setMaxDepth(maxDepth)
                .setFeaturesCol(featureCols)
                .setLabelCol(labelCol);
    }

    public void train(){
        // this.model = alg.fit(this.ds);
        this.pModel = new Pipeline().setStages(new PipelineStage[]{this.alg}).fit(this.ds);
    }

    public void save(String path) throws IOException {
        //this.model.save(path);
        this.pModel.save(path);
    }

    @Override
    public Dataset<Row> predict(Dataset<Row> testSet) {
        return this.pModel.transform(testSet);
    }
}
