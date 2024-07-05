package client1;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import model.LiftRideEvent;
import producer.LiftRideGenerator;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleThreadClient {
    private static final int TOTAL_REQUESTS = 10_000;
    private static final int MAX_RETRIES = 5;
    // private static final String BASE_URL = "http://localhost:8080/SkiResortAPIService_war/";
    private static final String BASE_URL = "http://52.41.249.244:8080/SkiResortAPIService-1.0-SNAPSHOT/"; // ec2 tomcat servlet1 url
    // servlet
    // private static final String BASE_URL = "http://34.221.187.81:8080/SkiResortAPIService/";
    // springboot
    // private static final String BASE_URL = "http://34.221.187.81:8080/cs-6650-distributed-ski-resort-server-springboot-1.0-SNAPSHOT";
    private static final BlockingQueue<LiftRideEvent> eventQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {

        new Thread(new LiftRideGenerator(eventQueue, TOTAL_REQUESTS)).start();

        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(BASE_URL);
        SkiersApi skiersApi = new SkiersApi(apiClient);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < TOTAL_REQUESTS; i++) {
            int attempt = 0;
            boolean success = false;
            while (attempt < MAX_RETRIES && !success) {
                try {
                    LiftRideEvent event = eventQueue.take(); // Blocks until an element is available
                    LiftRide liftRide = new LiftRide();
                    liftRide.setLiftID(event.getLiftID());
                    liftRide.setTime(event.getTime());
                    ApiResponse<Void> response = skiersApi.writeNewLiftRideWithHttpInfo(
                            liftRide,
                            event.getResortID(), event.getSeasonID(), event.getDayID(),
                            event.getSkierID());

                    if (response.getStatusCode() == 201) {
                        // System.out.println("Lift ride logged successfully" );
                        success = true;
                    } else if (response.getStatusCode() >= 400 && response.getStatusCode() < 600) {
                        System.err.println("Attempt " + (attempt + 1) + " failed: HTTP " + response.getStatusCode());
                        attempt++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // Proper handling of thread interruption
                    return;
                } catch (Exception e) {
                    e.printStackTrace();  // Log other exceptions for debug purposes
                }
            }
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double throughput = TOTAL_REQUESTS / (totalTime / 1000.0);  // requests per second, arrival rate
        double averageLatency =  (totalTime / 1000.0) / TOTAL_REQUESTS; // Average Time in the System

        System.out.println("Total requests sent: " + TOTAL_REQUESTS);
        System.out.println("Total run time (wall time): " + totalTime/1000.0 + " s");
        System.out.println("Average latency per request: " + averageLatency + " s");
        System.out.println("Total throughput (requests per second): " + throughput );
    }
}
