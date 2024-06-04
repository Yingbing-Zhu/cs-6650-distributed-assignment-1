package producer;

import model.LiftRideEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/***
 * Generates a random skier lift ride event that can be used to form a POST request.
 */
public class LiftRideGenerator implements Runnable {
    private final BlockingQueue<LiftRideEvent> queue;
    private final int totalEvents;

    public LiftRideGenerator(BlockingQueue<LiftRideEvent> queue, int totalEvents) {
        this.queue = queue;
        this.totalEvents = totalEvents;
    }

    @Override
    public void run() {
        for (int i = 0; i < totalEvents; i++) {
            LiftRideEvent liftRide = new LiftRideEvent(
                    ThreadLocalRandom.current().nextInt(1, 100001), // skierID
                    ThreadLocalRandom.current().nextInt(1, 11),    // resortID
                    ThreadLocalRandom.current().nextInt(1, 41),    // liftID
                    "2024",                                         // seasonID
                    "1",                                            // dayID
                    ThreadLocalRandom.current().nextInt(1, 361)    // time
            );
            try {
                queue.put(liftRide);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

