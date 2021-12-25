/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import breeze.linalg.DenseMatrix;
import breeze.linalg.DenseVector;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.ann.FunctionalLayer;
import org.apache.spark.ml.ann.LayerModel;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.mllib.util.MLUtils;

/**
 *
 * @author DevShop
 */
import org.apache.spark.ml.ann.ActivationFunction;
import scala.Function1;

public class LearnModel implements LayerModel {

    LearnModel() {
        FunctionalLayer oo = new FunctionalLayer((ActivationFunction) new Object());
    }

    @Override
    public DenseVector<Object> weights() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void eval(DenseMatrix<Object> dm, DenseMatrix<Object> dm1) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void computePrevDelta(DenseMatrix<Object> dm, DenseMatrix<Object> dm1, DenseMatrix<Object> dm2) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void grad(DenseMatrix<Object> dm, DenseMatrix<Object> dm1, DenseVector<Object> dv) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void createActivatinFunction() {
        ActivationFunction af = new ActivationFunction() {
            @Override
            public Function1<Object, Object> eval() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Function1<Object, Object> derivative() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

}
