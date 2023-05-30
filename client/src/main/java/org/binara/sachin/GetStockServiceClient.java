package org.binara.sachin;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.binara.sachin.grpc.generated.GetStockRequest;
import org.binara.sachin.grpc.generated.GetStockResponse;
import org.binara.sachin.grpc.generated.GetStockServiceGrpc;

public class GetStockServiceClient {
    private ManagedChannel channel = null;
    GetStockServiceGrpc.GetStockServiceBlockingStub clientStub = null;
    String host = null;
    int port = -1;

    public GetStockServiceClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void initializeConnection () {
        System.out.println("Initializing Connecting to server at " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        clientStub = GetStockServiceGrpc.newBlockingStub(channel);
    }
    public void closeConnection() {
        channel.shutdown();
    }

    public void processUserRequests() throws InterruptedException {
        while (true) {
            System.out.println("Requesting server to check the account balance for " + Constants.PRODUCT_NAME);
            GetStockRequest request = GetStockRequest
                    .newBuilder()
                    .setProductName(Constants.PRODUCT_NAME)
                    .build();
            GetStockResponse response = clientStub.getStock(request);
            System.out.printf(Constants.PRODUCT_NAME + " has " + response.getStock() + " units in stock\n");
            Thread.sleep(1000);
        }
    }
}
