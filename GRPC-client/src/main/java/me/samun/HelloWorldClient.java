package me.samun;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.*;
import io.grpc.internal.DnsNameResolverProvider;

import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;


public class HelloWorldClient {

    private static final Logger logger = Logger.getLogger(HelloWorldClient.class.getName());

    private final boolean hedging;

    static final String ENV_DISABLE_HEDGING = "DISABLE_HEDGING_IN_HEDGING_EXAMPLE";

    private final AtomicInteger failedRpcs = new AtomicInteger();
    private final PriorityBlockingQueue<Long> latencies = new PriorityBlockingQueue<>();

    private final ManagedChannel originChannel; //一个gRPC信道
    private final GreeterGrpc.GreeterBlockingStub blockingStub;//阻塞/同步 存根

    //初始化信道和存根


    public HelloWorldClient(String host, int port, boolean hedging) {

        this.hedging = hedging;

        String target = "dns:///" + host + ":" + port;
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder
                .forTarget(target)
//                .forAddress(host, port)
                .nameResolverFactory(new DnsNameResolverProvider());

        Map<String, ?> hedgingServiceConfig =
                new Gson()
                        .fromJson(
                                new JsonReader(
                                        new InputStreamReader(
                                                HelloWorldClient.class.getClassLoader().getResourceAsStream(
                                                        "hedging_service_config.json"),
                                                UTF_8)),
                                Map.class);

        if (hedging) {
            channelBuilder.defaultServiceConfig(hedgingServiceConfig);
        }

        originChannel = channelBuilder
                .usePlaintext()
                .build();

        ClientInterceptor interceptor = new HeaderClientInterceptor();
        Channel channel = ClientInterceptors.intercept(originChannel, interceptor);
        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    /**
     * Construct client for accessing RouteGuide server using the existing channel.
     */
//    private HelloWorldClient(ManagedChannelBuilder<?> channelBuilder) {
//        originChannel = channelBuilder.build();
//        blockingStub = GreeterGrpc.newBlockingStub(originChannel);
//    }
    public void shutdown() throws InterruptedException {
        originChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    //客户端方法
    public void greet(String name) {
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        HelloReply response;
        StatusRuntimeException statusRuntimeException = null;
        long startTime = System.nanoTime();
        try {
            response = blockingStub.withCompression("gzip").sayHello(request);
        } catch (StatusRuntimeException e) {
            failedRpcs.incrementAndGet();
            statusRuntimeException = e;
            System.out.println("RPC调用失败:" + e.getMessage());
            return;
        }

        long latencyMills = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        latencies.offer(latencyMills);

        if (statusRuntimeException == null) {
            logger.log(
                    Level.INFO,
                    "Greeting: {0}. Latency: {1}ms",
                    new Object[]{response.getMessage(), latencyMills});
        } else {
            logger.log(
                    Level.INFO,
                    "RPC failed: {0}. Latency: {1}ms",
                    new Object[]{statusRuntimeException.getStatus(), latencyMills});
        }
    }

    void printSummary() {
        int rpcCount = latencies.size();
        long latency50 = 0L;
        long latency90 = 0L;
        long latency95 = 0L;
        long latency99 = 0L;
        long latency999 = 0L;
        long latencyMax = 0L;
        for (int i = 0; i < rpcCount; i++) {
            long latency = latencies.poll();
            if (i == rpcCount * 50 / 100 - 1) {
                latency50 = latency;
            }
            if (i == rpcCount * 90 / 100 - 1) {
                latency90 = latency;
            }
            if (i == rpcCount * 95 / 100 - 1) {
                latency95 = latency;
            }
            if (i == rpcCount * 99 / 100 - 1) {
                latency99 = latency;
            }
            if (i == rpcCount * 999 / 1000 - 1) {
                latency999 = latency;
            }
            if (i == rpcCount - 1) {
                latencyMax = latency;
            }
        }

        logger.log(
                Level.INFO,
                "\n\nTotal RPCs sent: {0}. Total RPCs failed: {1}\n"
                        + (hedging ? "[Hedging enabled]\n" : "[Hedging disabled]\n")
                        + "========================\n"
                        + "50% latency: {2}ms\n"
                        + "90% latency: {3}ms\n"
                        + "95% latency: {4}ms\n"
                        + "99% latency: {5}ms\n"
                        + "99.9% latency: {6}ms\n"
                        + "Max latency: {7}ms\n"
                        + "========================\n",
                new Object[]{
                        rpcCount, failedRpcs.get(),
                        latency50, latency90, latency95, latency99, latency999, latencyMax});

        if (hedging) {
            logger.log(
                    Level.INFO,
                    "To disable hedging, run the client with environment variable {0}=true.",
                    ENV_DISABLE_HEDGING);
        } else {
            logger.log(
                    Level.INFO,
                    "To enable hedging, unset environment variable {0} and then run the client.",
                    ENV_DISABLE_HEDGING);
        }
    }

    public static void main(String[] args) throws Exception {
        HelloWorldClient client = new HelloWorldClient("grpc.samun.me", 80, true);
        try {
            for (int i = 0; i < 100; i++) {
                client.greet("world:" + i);
            }
        } finally {
            client.shutdown();
        }
//
//        Test2 t2 = Test2.newBuilder().setA(150).build();
//        for (byte b : t2.toByteArray()) {
//            System.out.println(b);
//        }
//
//        Test2 t3 = Test2.parseFrom(t2.toByteArray());
//
//        System.out.println(t3.getA());
//        boolean hedging = !Boolean.parseBoolean(System.getenv(ENV_DISABLE_HEDGING));
//        final HelloWorldClient client = new HelloWorldClient("localhost", 50051, hedging);
//        ForkJoinPool executor = new ForkJoinPool();
//
//        for (int i = 0; i < 20; i++) {
//            final String userId = "user" + i;
//            executor.execute(
//                    new Runnable() {
//                        @Override
//                        public void run() {
//                            client.greet(userId);
//                        }
//                    });
//        }
//
//        executor.awaitQuiescence(100, TimeUnit.SECONDS);
//        executor.shutdown();
//        client.printSummary();
//        client.shutdown();
    }
}
