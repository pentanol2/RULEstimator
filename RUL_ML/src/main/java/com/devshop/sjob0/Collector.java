package com.devshop.sjob0;

import static com.devshop.sjob0.RunFExctract.ss;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 *
 * @author DevShop
 */
// collect data from dataset
public class Collector {

    private String[] dataPaths = null;
    private ArrayList<Dataset<Row>> datasets = null;
    public SparkSession ss = null;

    Collector(SparkSession ss, String[] data_paths, StructType schema) {
        this.ss = ss;
        this.dataPaths = new String[data_paths.length];
        for (int i = 0; i < data_paths.length; i++) {
            if (Files.exists(Paths.get(data_paths[i]))) {
                this.dataPaths[i] = data_paths[i];
            } else {
                System.out.println("Data file " + data_paths[i] + " does not exist.");
                break;
            }
        }
        this.datasets = readDatasets(this.dataPaths, schema);
        System.out.println("Dataset successfully readed!");
    }

    public static Column[] colsAsObjects(String[] cols){
        Column[] c = new Column[cols.length];
        for (int i=0;i<cols.length;i++){
            c[i] = col(cols[i]);
        }
        return c;
    }

    // read data from dataset
    private ArrayList<Dataset<Row>> readDatasets(String[] paths, StructType schema) {
        ArrayList<Dataset<Row>> ds = new ArrayList();
        int size = paths.length;
        Dataset<Row> tmp;
        for (int i = 0; i < size; i++) {

            tmp = this.ss.read().option("delimiter", " ").schema(schema).csv(paths[i]);
            ds.add(tmp);
        }
        return ds;
    }

    // get dataset by index
    public Dataset<Row> getDataset(int i) {
        // return from array list
        return this.datasets.get(i);
    }

    public String toString() {
        String str = "Collected datasets:\n";
        for (int i = 0; i < this.datasets.size(); i++) {
            str = str + "Dataset " + (i + 1) + " with " + this.datasets.get(i).count() + " rows and "+this.datasets.get(i).columns().length+" columns.\n";
        }
        return str;
    }

}
