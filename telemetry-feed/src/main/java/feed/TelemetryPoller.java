package feed;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TelemetryPoller {
    public TelemetryPoller() {
    }

    public static void main(String[] args) {
        new TelemetryPoller().run();
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(TelemetryPoller.class);

        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String propsPath = rootPath + "config.properties";
        BlockingQueue<SensorReading> queue = new LinkedBlockingDeque<>(1000);

        // simulate machine sensory reading

        // create a kafka producer

        // shutdown hook

    }
}