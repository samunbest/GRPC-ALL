package me.samun;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.Random;

public class LatencyInjectionInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        int random = new Random().nextInt(100);
        long delay = 0;
        if (random < 1) {
            delay = 10_000;
        } else if (random < 5) {
            delay = 5_000;
        } else if (random < 10) {
            delay = 2_000;
        }

        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return serverCallHandler.startCall(serverCall,metadata);
    }
}
