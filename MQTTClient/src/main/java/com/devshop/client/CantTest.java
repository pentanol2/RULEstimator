/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.Response;
import java.net.MalformedURLException;import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStream;
import java.util.HashMap;


/**
 *
 * @author DevShop
 */
public class CantTest {
    public static void main(String[] args) throws MalformedURLException{
        String  account, apiKey, apiKeyPassphrase;
        account = "d4ca89cb-e62e-48ee-b17c-5d60e2fe88cf-bluemix";
        apiKey = "DLErybKLa5JDz40judqHyeY8Dqi0Ye0Tkb9330kt92Rd";
        apiKeyPassphrase =  "0cfa19d100bf2d188d23a0205c3ab2487885bd0fe82ca0f0967c8c099317e559";
        MyClient client = new MyClient(account,apiKey,apiKeyPassphrase);
        System.out.println("!"+client.client.getAllDbs());
       // client.client.createDB("sensors");
        Database db = client.client.database("sensors", false);
        //db.createIndex("idnt");
        System.out.println("The db's we have are "+client.client.getAllDbs());
        
        JsonObject jobj = new JsonObject();
        jobj.addProperty("_id", "1");
        jobj.addProperty("sensor", "1");
        jobj.addProperty("frequency","123.32");
        jobj.addProperty("date","15-23-15");
        //jobj.add("json-array", new JsonArray());
        //HashMap<String,JsonObject> mpobj = new HashMap<String,JsonObject>();
        //mpobj.put("sensor", jobj);
        //System.out.println("===================== >"+jobj.toString());
        //Response response = db.save(jobj);
        //System.out.println("Response "+ response.getError());
        
        JsonObject nol = db.find(JsonObject.class,"1");
        System.out.println("***** > "+db.getAllDocsRequestBuilder()+" -- "+ nol.get("frequency"));
        System.out.println("Saved!");
    }
    
}
