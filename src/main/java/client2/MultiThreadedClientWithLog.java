package client2;

import io.swagger.client.ApiClient;
import model.LiftRideEvent;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import producer.LiftRideGenerator;

import java.awt.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedClientWithLog {
    private static final int NUM_THREADS_INITIAL = 32;
    private static final int REQUESTS_PER_THREAD = 1000; //1000
    private static final int TOTAL_REQUESTS = 200_000;
    private static final int MAX_RETRIES = 5;
    private static final String BASE_URL = "http://35.90.169.232:8080/SkiResortAPIService/";
    private static final String logPath = "logs/logfile.csv";

    private static void runClient(int additionalThreads) {
        AtomicInteger successfulRequests = new AtomicInteger(); // record successful requests
        AtomicInteger remainingRequests = new AtomicInteger(TOTAL_REQUESTS);
        BlockingQueue<LiftRideEvent> eventQueue = new LinkedBlockingQueue<>();
        BlockingQueue<String> logQueue = new LinkedBlockingQueue<>();

        // thread to produce event
        new Thread(new LiftRideGenerator(eventQueue, TOTAL_REQUESTS)).start();

        // thread to write records
        Thread logWriterThread = new Thread(new LogWriter(logQueue, logPath));
        logWriterThread.start();

        ExecutorService executorService = Executors.newCachedThreadPool();

        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);
        CountDownLatch oldLatch = new CountDownLatch(NUM_THREADS_INITIAL);

        long startTime = System.currentTimeMillis();

        // Start initial batch of threads
        for (int i = 0; i < NUM_THREADS_INITIAL; i++) {
            ApiClient apiClient = new ApiClient();
            apiClient.setBasePath(BASE_URL);
            remainingRequests.addAndGet(-REQUESTS_PER_THREAD);
            completionService.submit(new PostTaskWithLog(eventQueue, REQUESTS_PER_THREAD, apiClient, oldLatch,
                    successfulRequests, MAX_RETRIES, logQueue), null);
        }


        try {
            Future<Void> completedFuture = completionService.take();  // Wait for the first thread to complete
            completedFuture.get();  // Throws an exception if the underlying task did

            // Calculate request per thread based on remaining requests
            int requestsPerThread = remainingRequests.get() / additionalThreads;
            int leftOverRequests = remainingRequests.get() % additionalThreads;
            CountDownLatch newLatch = new CountDownLatch(additionalThreads);
            // Submit additional tasks
            for (int i = 0; i < additionalThreads; i++) {
                ApiClient apiClient = new ApiClient();
                apiClient.setBasePath(BASE_URL);
                int batchSize = requestsPerThread + (i == additionalThreads - 1 ? leftOverRequests : 0);;
                remainingRequests.addAndGet(-batchSize);
                completionService.submit(new PostTaskWithLog(eventQueue, batchSize, apiClient, newLatch,
                        successfulRequests, MAX_RETRIES, logQueue), null);
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
        System.out.println("Number of successful requests: " + successfulRequests.get());
        System.out.println("Number of unsuccessful requests: " + (TOTAL_REQUESTS - successfulRequests.get()));
        System.out.println("Total run time (wall time): " + (totalTime / 1000.0) + " s");
        System.out.println("Total throughput (requests per second): " + throughput );

    }
    public static void main(String[] args) throws FileNotFoundException {
        runClient(350); // 200 active threads

        // get response times
        List<Long> responseTimes = readColumnValues(logPath, 2);


        double mean = responseTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);
        System.out.println("Mean response time: " + mean + " ms");

        Collections.sort(responseTimes);
        int middle = responseTimes.size() / 2;
        double median = responseTimes.size() % 2 == 1 ? responseTimes.get(middle) :
                (responseTimes.get(middle-1) + responseTimes.get(middle)) / 2.0;
        System.out.println("Median response time: " + median + " ms");
        int index = (int) Math.ceil(99 / 100.0 * responseTimes.size()) - 1;
        double p99 = responseTimes.get(index);
        System.out.println("99th percentile response time: " + p99 + " ms");

        long min = Collections.min(responseTimes);
        long max = Collections.max(responseTimes);
        System.out.println("Minimum response time: " + min + " ms");
        System.out.println("Maximum response time: " + max + " ms");

        // get timestamps
        List<Long> timestamps = readColumnValues(logPath, 0);
        Map<Long, Integer> throughputMap = calculateThroughput(timestamps, 1000);
        drawPlot(throughputMap);
    }

    /*
        function to read long type column values into list
     */
    private static List<Long> readColumnValues(String filePath, int columnIndex) throws FileNotFoundException {
        List<Long> values = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",");
                values.add(Long.parseLong(columns[columnIndex]));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from log file.", e);
        }
        return values;
    }

    public static Map<Long, Integer> calculateThroughput(List<Long> timestamps, long interval) {
        Map<Long, Integer> throughputMap = new TreeMap<>();
        for (long timestamp : timestamps) {
            long roundedTime = (timestamp / interval) * interval; // Round to the nearest interval
            throughputMap.merge(roundedTime, 1, Integer::sum); // Count requests per interval
        }
        return throughputMap;
    }

    private static void drawPlot(Map<Long, Integer> throughputMap) {
        // Convert the map to lists for plotting
        List<Date> xData = new ArrayList<>();
        List<Integer> yData = new ArrayList<>();

        throughputMap.forEach((key, value) -> {
            xData.add(new Date(key));
            yData.add(value);
        });

        // create chart
        XYChart chart = new XYChartBuilder()
                .width(800)
                .height(600)
                .title("Throughput Over Time")
                .xAxisTitle("Time")
                .yAxisTitle("Requests per Second")
                .build();
        chart.getStyler().setDatePattern("HH:mm:ss");
        chart.getStyler().setChartBackgroundColor(Color.WHITE);

        chart.addSeries("Throughput", xData, yData);

        // Show the chart
        new SwingWrapper(chart).displayChart();
    }

}


