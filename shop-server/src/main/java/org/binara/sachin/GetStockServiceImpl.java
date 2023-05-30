package org.binara.sachin;

import io.grpc.stub.StreamObserver;
import org.binara.sachin.grpc.generated.GetStockRequest;
import org.binara.sachin.grpc.generated.GetStockResponse;
import org.binara.sachin.grpc.generated.GetStockServiceGrpc;

public class GetStockServiceImpl extends GetStockServiceGrpc.GetStockServiceImplBase {
    private ShopServer shopServer;

    public GetStockServiceImpl(ShopServer shopServer) {
        this.shopServer = shopServer;
    }

    @Override
    public void getStock(GetStockRequest request, StreamObserver<GetStockResponse> responseObserver) {

        String productName = request.getProductName();
        int stock = getStock(productName);
        GetStockResponse response = GetStockResponse
                .newBuilder()
                .setStock(stock)
                .build();
        System.out.println("GetStockServiceImpl: getStock: " + productName + " " + stock);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private int getStock(String productName) {
        return shopServer.getProductStock();
    }
}
