package me.samun;

import com.google.common.base.Verify;
import com.google.common.base.VerifyException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.rpc.DebugInfo;
import io.grpc.*;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutionException;

public class DetailErrorSample {

    private static final Metadata.Key<DebugInfo> DEBUG_INFO_TRAILER_KEY =
            ProtoUtils.keyForProto(DebugInfo.getDefaultInstance());

    private ManagedChannel channel;
    private int port = 80;

    private static final DebugInfo DEBUG_INFO =
            DebugInfo.newBuilder()
                    .addStackEntries("stack_entry_1")
                    .addStackEntries("stack_entry_2")
                    .addStackEntries("stack_entry_3")
                    .setDetail("detailed error info.").build();

    private static final String DEBUG_DESC = "detailed error description";


    public void run() throws Exception {
        Server server = ServerBuilder.forPort(port).addService(new GreeterGrpc.GreeterImplBase() {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                super.sayHello(request, responseObserver);
            }
        }).build();

        channel = ManagedChannelBuilder.forAddress("127.0.0.1", 0).usePlaintext()
                .nameResolverFactory(new DnsNameResolverProvider()).build();

//        blockingCall();
        futureCallDirect();

    }


    public static void main(String[] args) throws Exception {
        new DetailErrorSample().run();
    }


    void blockingCall() {
        GreeterGrpc.GreeterBlockingStub stub = GreeterGrpc.newBlockingStub(channel);
        try {
            stub.sayHello(HelloRequest.newBuilder().build());
        } catch (Exception e) {
            verifyErrorReply(e);
        }
    }

    void futureCallDirect() {
        GreeterGrpc.GreeterFutureStub stub = GreeterGrpc.newFutureStub(channel);
        ListenableFuture<HelloReply> response =
                stub.sayHello(HelloRequest.newBuilder().setName("Lisa").build());

        try {
            response.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Status status = Status.fromThrowable(e.getCause());
            Verify.verify(status.getCode() == Status.Code.INTERNAL);
            Verify.verify(status.getDescription().contains("Xerxes"));
            // Cause is not transmitted over the wire.
        }
    }

    static void verifyErrorReply(Throwable t) {
        Status status = Status.fromThrowable(t);
        Metadata trailers = Status.trailersFromThrowable(t);
        Verify.verify(Status.Code.UNAVAILABLE == status.getCode());
        Verify.verify(trailers.containsKey(DEBUG_INFO_TRAILER_KEY));
        Verify.verify(status.getDescription().equals(DEBUG_DESC));
        try {
            Verify.verify(trailers.get(DEBUG_INFO_TRAILER_KEY).equals(DEBUG_INFO));
        } catch (IllegalArgumentException e) {
            throw new VerifyException(e);
        }
    }

}
