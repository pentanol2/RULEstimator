package com.devshop.client;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 *
 * @author DevShop
 */
public class MqttPublish {
    
    public static void main(String[] args) {
        String topic = "sensor1";
        String content = "This is a message: 1990";
        int qos = 2;
        String broker = "tcp://198.12.149.185:1883";
        String clientId = "pentanol1";
        MemoryPersistence persistence = new MemoryPersistence();
        try {
            MqttClient client = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + broker);
            //System.out.println("Clinet "+client.toString());
            client.connect(connOpts);
            System.out.println("Connected");
            System.out.println("Publishing message --> " + content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            client.publish(topic, message);
            System.out.println("Message published");
            message.setPayload("We love Asia".getBytes());
            client.publish(topic, message);
            System.out.println("Message published");
            message.setPayload("We love Africa".getBytes());
            client.publish(topic, message);
            System.out.println("Message published");
            message.setPayload("This is Arabia".getBytes());
            client.publish(topic, message);
            System.out.println("Message published");
            client.disconnect();
            System.out.println("Disconnected");
            System.exit(0);
            
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }
    
}
