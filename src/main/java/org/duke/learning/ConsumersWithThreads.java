package org.duke.learning;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ConsumersWithThreads {
    private static String bootstrapServers = "localhost:9092";
    private static String groupId = "meters2";

    public static Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }



    public ConsumersWithThreads() {

    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch
        );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            System.out.println("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("App interrupted");
            throw new RuntimeException(e);
        } finally {
            System.out.println("Application is clossing");
        }
    }

    public static void subscribeToConsumption() {

    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch){
            this.latch = latch;
             consumer = new KafkaConsumer<>(consumerConfig());
            consumer.subscribe(Collections.singleton("86434"));
        }
        @Override
        public void run() {
            try{
                while (true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records){
                        System.out.println(record.key() + " " + record.value());
                        // publish to mqtt
                    }
                }
            } catch (WakeupException e) {
                System.out.println("info received shutdown signal");
            } finally {
                consumer.close();
//                tell main code we're done with consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            consumer.wakeup(); // interrupt consumer.poll
        }
    }
}
