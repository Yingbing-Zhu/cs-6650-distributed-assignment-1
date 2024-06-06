package client1;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import model.LiftRideEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/***
 *  Post Task of each thread
 */
class PostTask implements Runnable {
    private final ApiClient apiClient;
    private final CountDownLatch latch;
    private final BlockingQueue<LiftRideEvent> eventQueue;
    private final AtomicInteger successfulRequests;
    private final int maxRetries;
    private final int requestsPerThread;

    public PostTask(BlockingQueue<LiftRideEvent> eventQueue, int requestsPerThread, ApiClient apiClient, CountDownLatch latch,
                    AtomicInteger successfulRequests, int maxRetries) {
        this.apiClient = apiClient;
        this.requestsPerThread = requestsPerThread;
        this.eventQueue = eventQueue;
        this.latch = latch;
        this.successfulRequests = successfulRequests;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        SkiersApi skiersApi = new SkiersApi(apiClient);
        int count = 0;
        while (count < requestsPerThread)  {
            int attempt = 0;
            boolean success = false;
            LiftRideEvent event;
            try {
                event = eventQueue.take(); // Blocks until an element is available
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (attempt < maxRetries && !success) {
                try {
                    LiftRide liftRide = new LiftRide();
                    liftRide.setLiftID(event.getLiftID());
                    liftRide.setTime(event.getTime());
                    ApiResponse<Void> response = skiersApi.writeNewLiftRideWithHttpInfo(
                            liftRide,
                            event.getResortID(), event.getSeasonID(), event.getDayID(),
                            event.getSkierID());

                    count ++;
                    if (response.getStatusCode() == 201) {
                        // System.out.println("Lift ride logged successfully" + event.getSkierID());
                        successfulRequests.incrementAndGet();
                        success = true;
                    } else if (response.getStatusCode() >= 400 && response.getStatusCode() < 600) {
                        System.err.println("Attempt " + (attempt + 1) + " failed: HTTP " + response.getStatusCode());
                        attempt++;
                    }
                } catch ( ApiException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        latch.countDown();
    }
}

