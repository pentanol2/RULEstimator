/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.devshop.sjob0;



import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.max;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.sql.execution.datasources.json.JsonOutputWriter;


/**
 * @author YOUSSEF AIDANI
 * Delete all the non effective features
 * Perform data normalization
 * Return a dataframe ready for training / testing
 */     
public class Preprocess {
    public static Dataset<Row> drop_colums(Dataset<Row> dataset,String[] cols){
        Dataset<Row> ds = null;
        ds = dataset.drop(cols);
        return ds;
    }

    public static Dataset<Row> calcRUL(Dataset<Row> ds){
        Dataset<Row> rul_cal = ds.groupBy("unit_no").agg(max("cycle").as("max_rul"));
        rul_cal = ds.join(rul_cal,ds.col("unit_no").equalTo(rul_cal.col("unit_no"))).withColumn("rul",expr("max_rul - cycle"));
        return rul_cal;
    }

    public static PipelineModel makeMinMaxModel(Dataset<Row> ds, String label){
        // VectorAssembler va = new VectorAssembler().setInputCols(ds.drop(label).columns()).setOutputCol("v_assmbl");
        // VectorAssembler va = new VectorAssembler().setInputCols(ds.drop(label).columns()).setOutputCol("v_assmbl");
        // return new MinMaxScaler().setInputCol("v_assmbl").setOutputCol("norm_vect").fit(va.transform(ds));
        Pipeline ppl = new Pipeline();
        ppl.setStages(new PipelineStage[]{
                new VectorAssembler().setInputCols(ds.drop(label).columns()).setOutputCol("v_assmbl"),
                new MinMaxScaler().setInputCol("v_assmbl").setOutputCol("norm_vect")
        });
        return ppl.fit(ds);
    }

    public static Dataset<Row> normalizeByColumnDS(Dataset<Row>ds,String[]cols){
        Dataset<Row> norm_dataset = ds;
        MinMaxScaler nrm = null;
        VectorAssembler va = null;
        for (String c:cols){
            va = new VectorAssembler().setInputCols(new String[]{c}).setOutputCol(c+"_vect");
            nrm = new MinMaxScaler().setInputCol(c+"_vect").setOutputCol(c+"_norm");
            norm_dataset = va.transform(norm_dataset);
            norm_dataset = nrm.fit(norm_dataset).transform(norm_dataset);
            norm_dataset = norm_dataset.drop(c,c+"_vect").withColumnRenamed(c+"_norm",c);
            norm_dataset = norm_dataset.drop(c);
            norm_dataset = norm_dataset.withColumnRenamed(c+"_prime",c);
        }
        return norm_dataset;

    }
    // pass a dataset a feature normalizer and a label to except
    public static Dataset<Row> normalizeByColumnVect(Dataset<Row> ds,PipelineModel mmsm ,String[] cols,String label){
        Dataset<Row> norm_dataset = ds;
        if (mmsm == null) {
            mmsm = makeMinMaxModel(norm_dataset,"rul");
            System.out.println("Creating Model : ");
            for (int i=0; i<mmsm.stages().length;i++) {
                System.out.println(("Stage "+i+" : " + mmsm.stages()[i]).trim());
            }
        }
        norm_dataset = mmsm.transform(norm_dataset);
        norm_dataset = norm_dataset.drop(ds.drop(label).columns()).drop("v_assmbl");
        return norm_dataset;
    }
}
