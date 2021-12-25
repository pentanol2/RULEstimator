/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 *
 * @author DevShop
 */
public class ClientThread extends TimerTask {

    // private Timer time = new Timer(); // Instantiate Timer Object
    //private ScheduledTask st = new ScheduledTask(); // Instantiate SheduledTask class
    private final FileParser fp;
    private final String threadName;
    private final String mqttBroker = "tcp://198.12.149.185:1883"; // default broker address
    private final MqttMessage message;
    private final MqttClient client;
    Thread thread;

    ClientThread(String fileName, String threadName) throws FileNotFoundException, MqttException {
        this.fp = new FileParser(fileName);
        this.threadName = threadName;
        this.thread = new Thread(this, threadName);
        System.out.println("Thread " + this.threadName + " created!");
        this.client = this.createMqttClient(mqttBroker);
        this.message = new MqttMessage();
        message.setQos(2);
        this.thread.start();

    }

    @Override
    public void run() {
        //time.schedule (this, 0,30000);
        String line;
        try {

            while (((line = this.fp.getLine()) != null)) {
                System.out.println(this.threadName + " : " + line);
                this.sendToBroker(line, this.threadName);
                Thread.sleep(1);

            }
        } catch (IOException ex) {
            Logger.getLogger(ClientThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(ClientThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MqttException ex) {
            Logger.getLogger(ClientThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private MqttClient createMqttClient(String broker) throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();

        MqttClient client = new MqttClient(broker, this.threadName, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        System.out.println("Connecting to broker: " + broker + " from client " + this.threadName);
        //System.out.println("Clinet "+client.toString());
        client.connect(connOpts);
        System.out.println("Connected");

        return client;
    }

    private boolean sendToBroker(String message, String topic) throws MqttException {
        System.out.println(" ***    " + message);
        this.message.setPayload(message.getBytes());
        System.out.println("Loaded: " + this.message);
        this.client.publish(topic, this.message);
        System.out.println("client Id " + this.client.getClientId());

        return true;
    }

}
