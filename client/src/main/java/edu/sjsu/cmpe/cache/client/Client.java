package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");

        CRDTClient client = new CRDTClient();

         client.put(1, "a");
         System.out.println("Sleep for 30 seconds after first write");
         Thread.sleep(30000);

         client.put(1, "b");
         System.out.println("Sleep for 30 seconds after second write");
         Thread.sleep(30000);

         String value = client.get(1);


         System.out.println("Existing Cache Client...");

}







}
