package org.binara.sachin;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import org.binara.sachin.grpc.generated.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class PurchaseProductServiceImpl extends PurchaseProductServiceGrpc.PurchaseProductServiceImplBase implements DistributedTxListener{
    private ShopServer server;
    private ManagedChannel channel = null;
    PurchaseProductServiceGrpc.PurchaseProductServiceBlockingStub clientStub = null;
    private String tempDataProductName = null;
    private int tempDataQuantity = 0;
    private boolean transactionStatus = false;

    public PurchaseProductServiceImpl(ShopServer shopServer) {
        this.server = shopServer;
    }

    private void startDistributedTx(String productName, int quantity) {
        try {
            server.getTransaction().start(productName, String.valueOf(UUID.randomUUID()));
            tempDataProductName = productName;
            tempDataQuantity = quantity;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void purchaseProduct(PurchaseProductRequest request, StreamObserver<PurchaseProductResponse> responseObserver) {
        String productName = request.getProductName();
        int quantity = request.getQuantity();

        if (server.isLeader()){
            // Act as primary
            try {
                System.out.println("Purchasing product as Primary");
                startDistributedTx(productName, quantity);
                updateSecondaryServers(productName, quantity);
                System.out.println("going to perform");
                if (quantity > 0){
                    ((DistributedTxCoordinator)server.getTransaction()).perform();
                } else {
                    ((DistributedTxCoordinator)server.getTransaction()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while purchasing product" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Purchasing product on a secondary, on Primary's command");
                startDistributedTx(productName, quantity);
                if (quantity != 0) {
                    ((DistributedTxParticipant)server.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant)server.getTransaction()).voteAbort();
                }
            } else {
                PurchaseProductResponse response = callPrimary(productName, quantity);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        PurchaseProductResponse response = PurchaseProductResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void updateStock() {
        if (tempDataProductName != null && tempDataQuantity != 0) {
            String productName = tempDataProductName;
            int quantity = tempDataQuantity;
            server.purchaseProduct(quantity);

            System.out.println("Product " + productName + " purchased. No of units: " + quantity + ". Transaction " +
                    "committed");

            tempDataProductName = null;
            tempDataQuantity = 0;
        }
    }

    private PurchaseProductResponse callServer(String productName, int quantity, boolean isSentByPrimary, String IPAddress,
            int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = PurchaseProductServiceGrpc.newBlockingStub(channel);

        PurchaseProductRequest request = PurchaseProductRequest
                .newBuilder()
                .setProductName(productName)
                .setQuantity(quantity)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        PurchaseProductResponse response = clientStub.purchaseProduct(request);
        return response;
    }

    private PurchaseProductResponse callPrimary(String productName, int quantity) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(productName, quantity, false, IPAddress, port);
    }

    private void updateSecondaryServers(String productName, int quantity) throws KeeperException, InterruptedException {
        System.out.println("Updating other servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(productName, quantity, true, IPAddress, port);
        }
    }

    @Override
    public void onGlobalCommit() {
        updateStock();
    }

    @Override
    public void onGlobalAbort() {
        tempDataProductName = null;
        tempDataQuantity = 0;

        System.out.println("Stock Update Aborted by the Coordinator");
    }
}
