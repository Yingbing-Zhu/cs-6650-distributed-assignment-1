package client1;

import io.swagger.client.ApiClient;
import model.LiftRideEvent;
import producer.LiftRideGenerator;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedClient {
    private static final int NUM_THREADS_INITIAL = 32; // initial number of requests
    private static final int REQUESTS_PER_THREAD = 1000; // initial requests per thread
    private static final int TOTAL_REQUESTS = 200_000; // total requests
    private static final int MAX_RETRIES = 5; // retries in api call
    // servlet
    // private static final String BASE_URL = "http://localhost:8080/SkiResortAPIService_war/"; // local test
    private static final String BASE_URL = "http://18.237.231.24:8080/SkiResortAPIService-1.0-SNAPSHOT/"; // ec2 tomcat servlet1 url
    // private static final String BASE_URL = "http://34.222.68.70:8080/SkiResortAPIService-1.0-SNAPSHOT/"; // ec2 tomcat servlet3 url
    // private static final String BASE_URL = "http://alb-1840563518.us-west-2.elb.amazonaws.com:8080/SkiResortAPIService-1.0-SNAPSHOT/"; // ec2 load balancer DNS name
    // springboot
    // private static final String BASE_URL = "http://34.221.187.81:8080/cs-6650-distributed-ski-resort-server-springboot-1.0-SNAPSHOT";

    private static double runClient(int additionalThreads) throws InterruptedException {
        AtomicInteger successfulRequests = new AtomicInteger(); // record successful requests
        BlockingQueue<LiftRideEvent> eventQueue = new LinkedBlockingQueue<>();
        new Thread(new LiftRideGenerator(eventQueue, TOTAL_REQUESTS)).start(); // the generate event finish early

        ExecutorService executorService = Executors.newCachedThreadPool(); // handle a large number of short-lived tasks. Examples include a web server handling sporadic and high-volume request loads where tasks complete quickly.

        CountDownLatch oldLatch = new CountDownLatch(NUM_THREADS_INITIAL);

        long startTime = System.currentTimeMillis();

        //  ----------  PHASE 1 - warm up with 32 threads ------------------
        // Start initial batch of threads for warm up
        for (int i = 0; i < NUM_THREADS_INITIAL; i++) {
            ApiClient apiClient = new ApiClient();
            apiClient.setBasePath(BASE_URL);
            executorService.submit(new PostTask(eventQueue, REQUESTS_PER_THREAD, apiClient, oldLatch,
                    successfulRequests, MAX_RETRIES));
        }

        //  ----------  PHASE 2 - submit additional 200 threads  ------------------
        int remaining = 168000;
        int requestsPerThread = remaining / additionalThreads;
        int leftOverRequests = remaining % additionalThreads;

        CountDownLatch newLatch = new CountDownLatch(additionalThreads);
        // Submit additional tasks
        for (int i = 0; i < additionalThreads; i++) {
            ApiClient apiClient = new ApiClient();
            apiClient.setBasePath(BASE_URL);
            int batchSize = requestsPerThread + (i == additionalThreads - 1 ? leftOverRequests : 0);
            executorService.submit(new PostTask(eventQueue, batchSize, apiClient, newLatch,
                    successfulRequests, MAX_RETRIES));
        }

        // wait for all threads to finish
        oldLatch.await(); // wait for the first batch to finish
        newLatch.await(); // wait for new batch to finish

        executorService.shutdown();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double totalThroughput = TOTAL_REQUESTS / (totalTime / 1000.0);

        System.out.println("Number of new threads created at 1st batch: " + NUM_THREADS_INITIAL);
        System.out.println("Number of new threads created at 2nd batch: " + additionalThreads);
        System.out.println("Max threads configured at Tomcat: 200");
        System.out.println("Number of successful requests: " + successfulRequests.get());
        System.out.println("Number of unsuccessful requests: " + (TOTAL_REQUESTS - successfulRequests.get()));
        System.out.println("Total run time (wall time): " + (totalTime / 1000.0) + " s");
        System.out.println("Total throughput (requests per second): " + totalThroughput);
        return totalThroughput;
    }

    public static void main(String[] args) throws InterruptedException {
        runClient(200);

        // 1st tryout:
        // int[] threadList = {50, 100, 150, 200};
        // best result is 100

//        double bestThroughput = 0;
//        double throughput;
//        int bestThreads = 32;
//
//        for (int threads: threadList) {
//            throughput = runClient(threads);
//            if (throughput > bestThroughput) {
//                bestThroughput = throughput;
//                bestThreads = threads;
//            }
//            System.out.println("---------------------------------");
//        }
//        System.out.println("Best Throughput: " + bestThroughput);
//        System.out.println("Best Number of  Thread: " + bestThreads);
    }
}