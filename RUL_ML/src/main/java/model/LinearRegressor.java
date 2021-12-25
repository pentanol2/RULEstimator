package model;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class LinearRegressor extends Model{

    private LinearRegression alg = null;
    private LinearRegressionModel model = null;
    private Dataset<Row> ds = null;

    LinearRegressor(Dataset<Row> ds,int maxIter,double regParam){
        this.ds = ds;
        this.alg = new LinearRegression()
                .setMaxIter(maxIter)
                .setRegParam(regParam);
    }

    public void train(){
        // model = this.alg.fit(ds);
        this.pModel = new Pipeline().setStages(new PipelineStage[]{this.alg}).fit(this.ds);
    }

    @Override
    public void save(String path) throws IOException {
        this.pModel.save(path);
    }

    @Override
    public Dataset<Row> predict(Dataset<Row> testSet) {
        return this.pModel.transform(testSet);
    }
}
