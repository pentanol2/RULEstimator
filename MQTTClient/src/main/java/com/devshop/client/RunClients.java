/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.io.FileNotFoundException;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 *
 * @author DevShop
 */
public class RunClients {

    public static void main(String[] args) throws FileNotFoundException, MqttException {
        String fileName1 = "C:\\mygrad\\python-ml-turbofan-master\\CMAPSSData\\train_FD001.txt";
        String fileName2 = "C:\\mygrad\\python-ml-turbofan-master\\CMAPSSData\\train_FD002.txt";
        String fileName3 = "C:\\mygrad\\python-ml-turbofan-master\\CMAPSSData\\train_FD003.txt";
        String fileName4 = "C:\\mygrad\\python-ml-turbofan-master\\CMAPSSData\\train_FD004.txt";

        ClientThread ct1 = new ClientThread(fileName1, "eunit");
      //  ClientThread ct2 = new ClientThread(fileName2, "sensor1");
       //ClientThread ct3 = new ClientThread(fileName3, "sensor3");
       // ClientThread ct4 = new ClientThread(fileName4, "sensor4");
        //new clientThread(fileName1,"sensor1");
        //new clientThread(fileName2,"sensor2");
        //new clientThread(fileName3,"sensor3");
        //new clientThread(fileName4,"sensor4");

        try {
            //ct1.run();
            //ct2.run();
            //ct3.run();
            //ct4.run();
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Main thread Interrupted" + e);
        }
        System.out.println("Main thread exiting.");
    }
}
