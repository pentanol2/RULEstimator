/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.Response;
import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 * @author DevShop
 */
public class databaseMod {

    public final CloudantClient client;
    public final Database dbm;

    public databaseMod(String defaultDb) throws MalformedURLException {
        String account, apiKey, apiKeyPassphrase;
        account = "d4ca89cb-e62e-48ee-b17c-5d60e2fe88cf-bluemix";
        apiKey = "DLErybKLa5JDz40judqHyeY8Dqi0Ye0Tkb9330kt92Rd";
        apiKeyPassphrase = "0cfa19d100bf2d188d23a0205c3ab2487885bd0fe82ca0f0967c8c099317e559";

        // this.client = ClientBuilder.account(account).username(apiKey).password(apiKeyPassphrase).build();
        this.client = ClientBuilder.url(new URL("https://d4ca89cb-e62e-48ee-b17c-5d60e2fe88cf-bluemix:0cfa19d100bf2d188d23a0205c3ab2487885bd0fe82ca0f0967c8c099317e559@d4ca89cb-e62e-48ee-b17c-5d60e2fe88cf-bluemix.cloudant.com")).username(account).password(apiKeyPassphrase).build();
        dbm = this.client.database("units", true);
    }

    //  database operations
    public Response createRecord() {
        return null;

    }

}
