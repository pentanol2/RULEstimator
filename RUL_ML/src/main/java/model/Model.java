package model;


import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.FileNotFoundException;
import java.io.IOException;

public abstract class Model {
    protected boolean freeze = false;
    protected PipelineModel pModel = null;
    public abstract void train();
    public abstract void save(String path) throws IOException;
    // public abstract void read(String);
    // public abstract T load(String modelPath);
    public abstract Dataset<Row> predict(Dataset<Row> testSet);
    // public abstract PipelineStage toPipelineStage();

}
