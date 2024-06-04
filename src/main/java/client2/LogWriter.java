package client2;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class LogWriter implements Runnable {
    private final BlockingQueue<String> logQueue;
    private final String filePath;

    public LogWriter(BlockingQueue<String> logQueue, String filePath) {
        this.logQueue = logQueue;
        this.filePath = filePath;
    }

    @Override
    public void run() {
        File file = new File(filePath);
        System.out.println("Writing to: " + file.getAbsolutePath());
        if (!file.getParentFile().exists()) {
            boolean dirsMade = file.getParentFile().mkdirs();
            System.out.println("Directories created: " + dirsMade);
        }
        //overwrite
        try (PrintWriter out = new PrintWriter(new FileWriter(filePath, false))) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    String logEntry = logQueue.take();
                    out.println(logEntry);
                    out.flush();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            flushRemainingLogs(out);
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
        System.out.println("Write complete!");
    }

    private void flushRemainingLogs(PrintWriter out) {
        while (!logQueue.isEmpty()) {
            out.println(logQueue.poll());
        }
        out.flush();
    }
}
