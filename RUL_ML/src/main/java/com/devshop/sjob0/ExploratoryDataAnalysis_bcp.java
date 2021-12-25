/**
 * @author YOUSSEF AIDANI
 */
package com.devshop.sjob0;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Hashtable;

import static org.apache.spark.sql.functions.*;

public class ExploratoryDataAnalysis_bcp {

    /*
EDA performs the following
calcultes the variance of each column
" the mean
" the quartiles
" the standard deviation
     */
    private Dataset<Row> dataset = null;
    private Hashtable<String,Dataset<Row>> summary = new Hashtable<String,Dataset<Row>>();

    ExploratoryDataAnalysis_bcp(Dataset<Row> dataset){
        this.dataset = dataset;
        this.execute();
    }

    ExploratoryDataAnalysis_bcp(Dataset<Row> dataset, String[] indices, String[] toIgnore) {
        this(dataset);
        this.execute();
    }

    public Hashtable<String,Dataset<Row>> getSummary(){
        return this.summary;
    }

    private void execute(){
        String[] columns = this.dataset.columns();
        for (String c :columns){
            if (c!="unit_no") {
                this.summary.put(c,this.dataset.agg(max(c).as("max"), min(c).as("min"), stddev(c).as("std")));
            }
        }
    }

    public ArrayList<String> check_invariant_features(double threshold){
        // check for invariance using
        ArrayList<String> invarian_cols = new ArrayList<String>();
        String[] columns = this.dataset.columns();
        double stdv = 0;
        for (String c:columns){
            if ( c!="unit_no") {
                stdv = this.summary.get(c).select("std").first().getDouble(0);
                if (stdv < threshold) {
                    invarian_cols.add((String) c);
                }
            }
        }
        return invarian_cols;
    }

    public Dataset<Row> absMin(String[] colnames) {
        // functions.min(this.dataset.col(colnames[0]));
        return null;
    }

    public Dataset<Row> absMax() {
        return null;
    }

    public Dataset<Row> absAvg() {
        // this.dataset
        return null;
    }

    public Dataset<Row> absCount() {
        return null;
    }

    public Dataset<Row> absVar() {
        return null;
    }

    public Dataset<Row> absStdev() {
        return null;
    }
    /*
    public Dataset<Row> min() {
        return null;
    }
    */

    /*
    public Dataset<Row> max() {
        return null;
    }
    */


    public Dataset<Row> avg() {
        return null;
    }

    public Dataset<Row> count() {
        return null;
    }

    public Dataset<Row> var() {
        return null;
    }

    public Dataset<Row> stdev() {
        return null;
    }

}
