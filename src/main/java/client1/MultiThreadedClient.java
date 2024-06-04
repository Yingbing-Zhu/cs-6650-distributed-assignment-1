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
    private static final String BASE_URL = "http://54.149.84.9:8080/SkiResortAPIService/"; // server url
    //private static final String BASE_URL = "http://localhost:8080/";
//    private static final AtomicInteger successfulRequests = new AtomicInteger(); // record successful requests
//    private static final AtomicInteger remainingRequests = new AtomicInteger(TOTAL_REQUESTS);
    // private static final BlockingQueue<LiftRideEvent> eventQueue = new LinkedBlockingQueue<>();

    private static double runClient(int requestsPerThreadNew) {
        AtomicInteger successfulRequests = new AtomicInteger(); // record successful requests
        AtomicInteger remainingRequests = new AtomicInteger(TOTAL_REQUESTS);
        BlockingQueue<LiftRideEvent> eventQueue = new LinkedBlockingQueue<>();
        new Thread(new LiftRideGenerator(eventQueue, TOTAL_REQUESTS)).start();

        ExecutorService executorService = Executors.newCachedThreadPool(); // handle a large number of short-lived tasks. Examples include a web server handling sporadic and high-volume request loads where tasks complete quickly.

        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);
        CountDownLatch oldLatch = new CountDownLatch(NUM_THREADS_INITIAL);

        long startTime = System.currentTimeMillis();

        // Start initial batch of threads
        for (int i = 0; i < NUM_THREADS_INITIAL; i++) {
            ApiClient apiClient = new ApiClient();
            apiClient.setBasePath(BASE_URL);
            remainingRequests.addAndGet(-REQUESTS_PER_THREAD);
            completionService.submit(new PostTask(eventQueue, REQUESTS_PER_THREAD, apiClient, oldLatch,
                    successfulRequests, MAX_RETRIES), null);
        }


        int additionalThreads;
        try {
            Future<Void> completedFuture = completionService.take();  // Wait for the first thread to complete
            completedFuture.get();  // Throws an exception if the underlying task did

            // Calculate additional threads needed based on remaining requests
            additionalThreads = (remainingRequests.get() + requestsPerThreadNew - 1) / requestsPerThreadNew;
            CountDownLatch newLatch = new CountDownLatch(additionalThreads);
            // Submit additional tasks
            for (int i = 0; i < additionalThreads; i++) {
                ApiClient apiClient = new ApiClient();
                apiClient.setBasePath(BASE_URL);
                int batchSize = Math.min(remainingRequests.get(), requestsPerThreadNew);
                remainingRequests.addAndGet(-batchSize);
                completionService.submit(new PostTask(eventQueue, batchSize, apiClient, newLatch,
                        successfulRequests, MAX_RETRIES), null);
            }
            newLatch.await();
            oldLatch.await();
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error in processing requests", e);
        } finally {
            executorService.shutdown();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double throughput = TOTAL_REQUESTS / (totalTime / 1000.0);  // requests per second
        System.out.println("Number of new threads created after initial batch: " + additionalThreads);
        System.out.println("Requests Per thread after initial batch: " + requestsPerThreadNew);
        System.out.println("Number of successful requests: " + successfulRequests.get());
        System.out.println("Number of unsuccessful requests: " + (TOTAL_REQUESTS - successfulRequests.get()));
        System.out.println("Total run time (wall time): " + (totalTime / 1000.0) + " s");
        System.out.println("Total throughput (requests per second): " + throughput );
        return throughput;

    }
    public static void main(String[] args)  {
        // best throughput
        runClient(440);
        // 1st tryout:
        // Best throughput: 2658 requests/second
        // 500 requests per thread/ 336 more threads
        // int[] requestsPerThreadList = {500, 800, 1000, 1500, 2000};

        // 2nd tryout: best it's 400 requests per thread/ 420 more threads
        // int[] requestsPerThreadList = {300, 400, 450, 500, 550, 600, 700};
        // 3rd tryout: best it's 440 requests per thread/ 420 more threads
//        int[] requestsPerThreadList = {360, 380, 400, 420, 440};
//        double bestThroughput = 0;
//        double throughput;
//        int bestRequestsPerThread = 1000;
//
//        for (int requests: requestsPerThreadList) {
//            throughput = runClient(requests);
//            if (throughput > bestThroughput) {
//                bestThroughput = throughput;
//                bestRequestsPerThread = requests;
//            }
//        }
//        System.out.println("Best Throughput: " + bestThroughput);
//        System.out.println("Best Requests Per Thread: " + bestRequestsPerThread);

    }

}

