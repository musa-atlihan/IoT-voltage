package feed;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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
        TelemetryRunnable telemetryRunnable = new TelemetryRunnable(queue);
        Thread telemetryThread = new Thread(telemetryRunnable);

        // create a kafka producer
        ProducerRunnable producerRunnable = new ProducerRunnable(propsPath, queue);
        Thread producerThread = new Thread(producerRunnable);

        telemetryThread.start();
        producerThread.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook.");
            producerRunnable.shutdown();
            logger.info("Application has exited.");
        }));

    }

    public class ProducerRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(ProducerRunnable.class);
        KafkaProducer<String, String> producer;
        BlockingQueue<SensorReading> queue;
        String topic;

        public ProducerRunnable(String propsPath, BlockingQueue<SensorReading> queue) {

            try {
                this.queue= queue;

                FileInputStream in = new FileInputStream(propsPath);

                Properties properties = new Properties();
                properties.load(in);
                this.topic = properties.getProperty("topic");
                in.close();

                producer = new KafkaProducer<String, String>(properties);
            } catch (IOException e) {
                logger.error("Kafka Producer properties file read failure!");
            }

        }

        @Override
        public void run() {

            while (true) {
                try {
                    SensorReading reading = queue.poll(5, TimeUnit.SECONDS);
                    logger.info("Producer message read " + reading.toString());
                    producer.send(new ProducerRecord<String, String>(topic, reading.toString()));
                } catch (InterruptedException e) {
                    logger.info("Producer interrupted! " + e);
                }
            }
        }

        public void shutdown() {
            producer.close();
            logger.info("Kafka Producer has closed.");
        }
    }

    public class TelemetryRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(TelemetryRunnable.class);
        private BlockingQueue<SensorReading> queue;

        public TelemetryRunnable(BlockingQueue<SensorReading> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while (true) {

                    SensorReading reading = new SensorReading();
                    reading.setMachineId(produceMachineId());
                    reading.setVoltage(produceSensorReading());

                    boolean success = queue.offer(reading, 100, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                logger.info("Telemetry runnable interrupted!");
            }

        }

        private Integer produceMachineId() {
            Random random = new Random();
            Integer number = random.nextInt(100);

            // simulate reading time
            try {
                Thread.sleep(random.nextInt(1000));
            } catch (InterruptedException e) {
                logger.info("MachineId generation interrupted!");
            }
            return number;
        }

        private Long produceSensorReading() {
            Random random = new Random();
            long leftLimit = 800L;
            long rightLimit = 1500L;

            long number = leftLimit + (long) (Math.random() * (rightLimit - leftLimit));

            // simulate reading time
            try {
                Thread.sleep(random.nextInt(1000));
            } catch (InterruptedException e) {
                logger.info("SensorReading generation interrupted!");
            }
            return number;
        }


    }

}