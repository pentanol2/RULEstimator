package com.devshop.client;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 *
 * @author DevShop
 */

public class MqttRecieve {
    public static class Callback implements MqttCallback{

        @Override
        public void connectionLost(Throwable thrwbl) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void messageArrived(String topic, MqttMessage mm) throws Exception {
            System.out.println("*************************");
           System.out.println(topic + ": " + Arrays.toString(mm.getPayload()));
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public void deliveryComplete(IMqttDeliveryToken imdt) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        };
    
}
    public static void main(String[] args) throws InterruptedException {
        String clientId = "pentanol2";
        String topic = "sensor1";
        String broker = "tcp://198.12.149.185:1883";
        Callback cbk = new Callback();
        MemoryPersistence persistence = new MemoryPersistence();
        try {
            MqttClient client = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + broker);
            client.setCallback(cbk);
            client.connect(connOpts);
            System.out.println("Connected");
            System.out.println("--------------------");
            client.subscribe(topic);
            CountDownLatch receivedSignal = new CountDownLatch(10);
            receivedSignal.await(1, TimeUnit.MINUTES);
            System.out.println("--------------------");
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
