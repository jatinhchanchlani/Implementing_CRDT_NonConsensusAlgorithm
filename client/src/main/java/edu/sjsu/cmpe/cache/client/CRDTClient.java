package edu.sjsu.cmpe.cache.client;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class CRDTClient
{

    private boolean node1Status = false, node2Status = false, node3Status = false;
    private int successfulWriteCounts =0;
    private String value1="", value2 ="", value3="",correctValue="";
    private CountDownLatch countDownLatch = new CountDownLatch(3);



    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    public String get(long key) throws Exception {
    System.out.println("reached in get");
    countDownLatch = new CountDownLatch(3);

    Future<HttpResponse<JsonNode>> future = Unirest.get("http://localhost:3000/cache/{key}")
    .header("accept", "application/json")
    .routeParam("key", Long.toString(key))
    .asJsonAsync(new Callback<JsonNode>() {

    public void failed(UnirestException e) {
        countDownLatch.countDown();
    }

    public void completed(HttpResponse<JsonNode> response) {
        value1 = response.getBody().getObject().getString("value").toString();
        System.out.println("value1 : " + value1);
        countDownLatch.countDown();

    }

    public void cancelled() {
        countDownLatch.countDown();
    }

});

//Calling Node 2


    Future<HttpResponse<JsonNode>> future2 = Unirest.get("http://localhost:3001/cache/{key}")
    .header("accept", "application/json")
    .routeParam("key", Long.toString(key))
    .asJsonAsync(new Callback<JsonNode>() {

    public void failed(UnirestException e) {
        countDownLatch.countDown();
    }

    public void completed(HttpResponse<JsonNode> response) {
        value2 = response.getBody().getObject().getString("value");
        System.out.println("value2 : " + value2);
        countDownLatch.countDown();
    }

    public void cancelled() {
        countDownLatch.countDown();
    }

});


// Calling Node 3
    Future<HttpResponse<JsonNode>> future3 = Unirest.get("http://localhost:3002/cache/{key}")
    .header("accept", "application/json")
    .routeParam("key", Long.toString(key))
    .asJsonAsync(new Callback<JsonNode>() {

    public void failed(UnirestException e) {
        countDownLatch.countDown();
    }

    public void completed(HttpResponse<JsonNode> response) {
        value3 = response.getBody().getObject().getString("value");
        System.out.println("value3 : " + value3);
        countDownLatch.countDown();
    }

    public void cancelled() {
        countDownLatch.countDown();
    }

});

countDownLatch.await(3,TimeUnit.SECONDS);

              if(value1.equalsIgnoreCase(value2))
                {
                    if(value1.equalsIgnoreCase(value3))
                    {
                    return value1;
                    }
                    else
                    {
                    node3Status = true;
                    correctValue = value1;
                    }
                }
                else if(value1.equalsIgnoreCase(value3))
                {
                    node2Status = true;
                    correctValue = value1;
                }
                else if(value2.equalsIgnoreCase(value3))
                {
                    node1Status = true;
                    correctValue = value2;
                }
                else
                {
                return "Server conflict.. Cannot Return value at the moment";
                }

        performReadRepair(key,correctValue);

          //    Unirest.shutdown();

        return correctValue;

 }




private void performReadRepair(long key, String correctValue)
{

   try
   {
    if(node1Status)
    {
        System.out.println("Repairing Node 1 with value :" + correctValue);

        HttpResponse<JsonNode> response = Unirest.put("http://localhost:3000" + "/cache/{key}/{value}")
        							.header("accept", "application/json")
        							.routeParam("key", Long.toString(key))
        							.routeParam("value", correctValue)
        							.asJson();
    }
    if(node2Status)
    {
        System.out.println("Repairing Node 2 with value :" + correctValue);
                HttpResponse<JsonNode> response = Unirest.put("http://localhost:3001" + "/cache/{key}/{value}")
                							.header("accept", "application/json")
                							.routeParam("key", Long.toString(key))
                							.routeParam("value", correctValue)
                							.asJson();
    }
    if(node3Status)
    {
           System.out.println("Repairing Node 3 with value :" + correctValue);
                        HttpResponse<JsonNode> response = Unirest.put("http://localhost:3002" + "/cache/{key}/{value}")
                        							.header("accept", "application/json")
                        							.routeParam("key", Long.toString(key))
                        							.routeParam("value", correctValue)
                        							.asJson();
    }
    }
    catch(Exception e)
    {

    }

}


    public void put(long key, String value) throws Exception {

System.out.println("value :" + value);
 countDownLatch = new CountDownLatch(3);
            // Calling the first node

            Future<HttpResponse<JsonNode>> future =  Unirest
                                                    .put("http://localhost:3000/cache/{key}/{value}")
                                                    .header("accept", "application/json")
                                                    .routeParam("key", Long.toString(key))
                                                    .routeParam("value", value)
              .asJsonAsync(new Callback<JsonNode>() {

                public void failed(UnirestException e) {
                countDownLatch.countDown();
                }

                public void completed(HttpResponse<JsonNode> response) {
                     //int code = response.getStatus();
                    successfulWriteCounts++;
                node1Status = true;
                countDownLatch.countDown();
                }

                public void cancelled() {
                countDownLatch.countDown();
                }


            });


            // Calling the second node

            future =  Unirest
                                                    .put("http://localhost:3001/cache/{key}/{value}")
                                                    .header("accept", "application/json")
                                                    .routeParam("key", Long.toString(key))
                                                    .routeParam("value", value)
              .asJsonAsync(new Callback<JsonNode>() {

                public void failed(UnirestException e) {
                countDownLatch.countDown();
                }

                public void completed(HttpResponse<JsonNode> response) {
                                        successfulWriteCounts++;
                                        node2Status = true;
                                        countDownLatch.countDown();
                }

                public void cancelled() {
                countDownLatch.countDown();
                }


            });


             // Calling the third node

            Future<HttpResponse<JsonNode>> future3 =  Unirest
                                                    .put("http://localhost:3002/cache/{key}/{value}")
                                                    .header("accept", "application/json")
                                                    .routeParam("key", Long.toString(key))
                                                    .routeParam("value", value)
              .asJsonAsync(new Callback<JsonNode>() {

                public void failed(UnirestException e) {
                countDownLatch.countDown();
                }

                public void completed(HttpResponse<JsonNode> response) {
                     successfulWriteCounts++;
                     node3Status = true;
                countDownLatch.countDown();
                }

                public void cancelled() {
                countDownLatch.countDown();
                }


            });


countDownLatch.await(3,TimeUnit.SECONDS);



if(successfulWriteCounts>=2)
                    {
                    //
                    System.out.println("Key: " + key + " Value: "+ value +" written successfully in three nodes ");

                    }
                    else
                    {
                        System.out.println("Cant replicate data");
                        HttpResponse<JsonNode> response = null;
                        HttpResponse<JsonNode> response1 = null;
                        HttpResponse<JsonNode> response2 = null;
                        try
                        {
                        if(node1Status)
                        {
                        System.out.println("Deleting key = " +key +" from http://localhost:3000 for consistency");
                        response = Unirest
                                        .delete("http://localhost:3000/cache/{key}")
                                        .routeParam("key", Long.toString(key)).asJson();

                        }
                        if(node2Status)
                        {
                        System.out.println("Deleting key = " +key +" from http://localhost:3001 for consistency");
                         response1  = Unirest
                                        .delete("http://localhost:3001/cache/{key}")
                                        .routeParam("key", Long.toString(key)).asJson();
                        }
                        if(node3Status)
                        {
                        System.out.println("Deleting key = " +key +" from http://localhost:3002 for consistency");
                         response2  = Unirest
                                         .delete("http://localhost:3002/cache/{key}")
                                         .routeParam("key", Long.toString(key)).asJson();
                        }

                        } catch (UnirestException e) {
                                System.err.println(e);
                            }
                    }


clearValues();

    }
    private void clearValues()
    {
    node1Status = false; node2Status = false; node3Status = false;
    successfulWriteCounts =0;
    }
}