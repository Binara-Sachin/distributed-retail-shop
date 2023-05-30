package org.binara.sachin;

public class MainClient {
    public static void main(String[] args) throws InterruptedException {
        int port = Integer.parseInt(args[0].trim());
        String operation = args[1];

        if (args.length != 3) {
            System.out.println("Usage Online retail shop client <host> <port> <s(et)|c(heck)");
            System.exit(1);
        }

        if ("g".equals(operation)) {
            GetStockServiceClient client = new GetStockServiceClient(Constants.HOST, port);
            client.initializeConnection();
            client.processUserRequests();
            client.closeConnection();
        }
    }
}