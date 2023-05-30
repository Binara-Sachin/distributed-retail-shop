package org.binara.sachin;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import org.binara.sachin.grpc.generated.AddStockRequest;
import org.binara.sachin.grpc.generated.AddStockResponse;
import org.binara.sachin.grpc.generated.AddStockServiceGrpc;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class AddStockServiceImpl extends AddStockServiceGrpc.AddStockServiceImplBase implements DistributedTxListener{
    private ShopServer server;
    private ManagedChannel channel = null;
    AddStockServiceGrpc.AddStockServiceBlockingStub clientStub = null;
    private String tempDataProductName = null;
    private int tempDataQuantity = 0;
    private boolean transactionStatus = false;

    public AddStockServiceImpl(ShopServer shopServer) {
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
    public void addStock(AddStockRequest request, StreamObserver<AddStockResponse> responseObserver) {
        String productName = request.getProductName();
        int quantity = request.getQuantity();

        if (server.isLeader()){
            // Act as primary
            try {
                System.out.println("Adding stock as Primary");
                startDistributedTx(productName, quantity);
                updateSecondaryServers(productName, quantity);
                System.out.println("going to perform");
                if (quantity > 0){
                    ((DistributedTxCoordinator)server.getTransaction()).perform();
                } else {
                    ((DistributedTxCoordinator)server.getTransaction()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while adding stock" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Adding stock on a secondary, on Primary's command");
                startDistributedTx(productName, quantity);
                if (quantity != 0) {
                    ((DistributedTxParticipant)server.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant)server.getTransaction()).voteAbort();
                }
            } else {
                AddStockResponse response = callPrimary(productName, quantity);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        AddStockResponse response = AddStockResponse
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
            server.addStockToProduct(quantity);

            System.out.println("Product " + productName + " added stock of " + quantity + " committed");

            tempDataProductName = null;
            tempDataQuantity = 0;
        }
    }

    private AddStockResponse callServer(String productName, int quantity, boolean isSentByPrimary, String IPAddress,
            int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = AddStockServiceGrpc.newBlockingStub(channel);

        AddStockRequest request = AddStockRequest
                .newBuilder()
                .setProductName(productName)
                .setQuantity(quantity)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        AddStockResponse response = clientStub.addStock(request);
        return response;
    }

    private AddStockResponse callPrimary(String productName, int quantity) {
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
