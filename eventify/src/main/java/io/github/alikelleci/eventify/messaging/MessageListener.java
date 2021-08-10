package io.github.alikelleci.eventify.messaging;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public interface MessageListener {

  Consumer<String, Message> getConsumer();

  void onMessage(ConsumerRecords<String, Message> consumerRecords);

  default void start() {
    AtomicBoolean closed = new AtomicBoolean(false);
    Consumer<String, Message> consumer = getConsumer();

    Thread thread = new Thread(() -> {
      try {
        //getConsumer().subscribe(Collections.singletonList(applicationId.concat(".results")));
        while (!closed.get()) {
          ConsumerRecords<String, Message> consumerRecords = consumer.poll(Duration.ofMillis(1000));
          onMessage(consumerRecords);
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
      } finally {
        consumer.close();
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      closed.set(true);
      consumer.wakeup();
    }));
    thread.start();
  }
}
