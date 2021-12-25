/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.org.lightcouch.CouchDbException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author DevShop
 */
public class MyClient {

    public final CloudantClient client;

    public MyClient(String account, String apiKey, String apiKeyPassphrase) throws MalformedURLException {
    
            // this.client = ClientBuilder.account(account).username(apiKey).password(apiKeyPassphrase).build();
            this.client = ClientBuilder.url(new URL("https://d4ca89cb-e62e-48ee-b17c-5d60e2fe88cf-bluemix:0cfa19d100bf2d188d23a0205c3ab2487885bd0fe82ca0f0967c8c099317e559@d4ca89cb-e62e-48ee-b17c-5d60e2fe88cf-bluemix.cloudant.com")).username(account).password(apiKeyPassphrase).build();
      
    }

    public boolean persistRecord() {
        System.out.println("sout -> " + this.client);
        return true;
    }

}
