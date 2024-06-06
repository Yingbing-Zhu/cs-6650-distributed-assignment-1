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
    private static final String BASE_URL = "http://35.90.169.232:8080/SkiResortAPIService/"; // server url
    //private static final String BASE_URL = "http://localhost:8080/";

    private static double runClient(int additionalThreads) {
        AtomicInteger successfulRequests = new AtomicInteger(); // record successful requests
        BlockingQueue<LiftRideEvent> eventQueue = new LinkedBlockingQueue<>();
        new Thread(new LiftRideGenerator(eventQueue, TOTAL_REQUESTS)).start(); // the generate event finish early

        ExecutorService executorService = Executors.newCachedThreadPool(); // handle a large number of short-lived tasks. Examples include a web server handling sporadic and high-volume request loads where tasks complete quickly.
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);


        CountDownLatch oldLatch = new CountDownLatch(NUM_THREADS_INITIAL);

        long startTime = System.currentTimeMillis();

        // Start initial batch of threads
        for (int i = 0; i < NUM_THREADS_INITIAL; i++) {
            ApiClient apiClient = new ApiClient();
            apiClient.setBasePath(BASE_URL);
            completionService.submit(new PostTask(eventQueue, REQUESTS_PER_THREAD, apiClient, oldLatch,
                    successfulRequests, MAX_RETRIES), null);
        }

//        long firstThreadCompletionTime;
//        int firstSuccessfulRequests;
//        long firstTime;
        try {
            Future<Void> completedFuture = completionService.take();  // Wait for the first thread to complete
            completedFuture.get();  // Throws an exception if the underlying task did

//            // Record the time when the first thread completes
//            firstThreadCompletionTime = System.currentTimeMillis();
//            firstTime = firstThreadCompletionTime - startTime;
//            firstSuccessfulRequests = successfulRequests.get();

            // Calculate request per thread based on remaining requests
            int remainingRequests = TOTAL_REQUESTS - REQUESTS_PER_THREAD * NUM_THREADS_INITIAL;
            int requestsPerThread = remainingRequests / additionalThreads;
            int leftOverRequests = remainingRequests % additionalThreads;

            CountDownLatch newLatch = new CountDownLatch(additionalThreads);
            // Submit additional tasks
            for (int i = 0; i < additionalThreads; i++) {
                ApiClient apiClient = new ApiClient();
                apiClient.setBasePath(BASE_URL);
                int batchSize = requestsPerThread + (i == additionalThreads - 1 ? leftOverRequests : 0);;
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
        double totalThroughput = TOTAL_REQUESTS / (totalTime / 1000.0);

//        // --------------------- 1st stage throughput -----------------------
//        double firstThroughput = firstSuccessfulRequests / (firstTime / 1000.0);
//        System.out.println("1st Stage throughput: " + firstThroughput );
//
//        // --------------------- 2nd stage throughput -----------------------
//        long secondThreadCompletionTime = endTime - firstThreadCompletionTime;
//        double secondThroughput = TOTAL_REQUESTS / (secondThreadCompletionTime / 1000.0);
//        System.out.println("2nd Stage throughput: " + secondThroughput );


        System.out.println("Number of new threads created after initial batch: " + additionalThreads);
        System.out.println("Number of successful requests: " + successfulRequests.get());
        System.out.println("Number of unsuccessful requests: " + (TOTAL_REQUESTS - successfulRequests.get()));
        System.out.println("Total run time (wall time): " + (totalTime / 1000.0) + " s");
        System.out.println("Total throughput (requests per second): " + totalThroughput );
        return totalThroughput;
    }

    public static void main(String[] args) throws InterruptedException {
        // load test
        int[] threadList = {300, 350};
        for (int threads: threadList) {
            runClient(threads);
        }
        // runClient(350); //
        // runClient(217); // throughput = 2167
        // runClient(300); // throughput = 2361
        // runClient(500); // throughput = 2541
        // runClient(168); // throughput = 1963
        // runClient(700); // throughput = 2438.6080425293244
        // ThreadCount: 121
        // PeakThreadCount: 121
        // TotalStartedThreadCount: 121


        // 1st tryout:
//        int[] threadList = {100, 300, 500};
        // 100: active threads = 73, avg throughput = 1614
        // 300: active threads = 148, avg throughput = 2686 (best result)
        // 500: active threads = 200, avg throughput = 2674


        // 2nd tryout:
        // int[] threadList = {200, 250, 300, 350, 400};
        // 200: active threads = 90, avg throughput = 2218
        // 250: active threads = 116, avg throughput = 2538
        // 300: active threads = 146, avg throughput = 2692
        // 350: active threads = 162, avg throughput = 2843 (best result)
        // 400: active threads = 200, avg throughput = 2456


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

