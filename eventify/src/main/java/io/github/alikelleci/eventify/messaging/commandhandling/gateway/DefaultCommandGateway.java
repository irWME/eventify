package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.MessageListener;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DefaultCommandGateway implements CommandGateway, MessageListener {

  //private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final Producer<String, Message> producer;
  private final Consumer<String, Message> consumer;
  private final String applicationId;

  public DefaultCommandGateway(Producer<String, Message> producer, Consumer<String, Message> consumer, String applicationId) {
    this.producer = producer;
    this.consumer = consumer;
    this.applicationId = applicationId;
    this.start();
  }

  @Override
  public void sendAndForget(Object payload, Metadata metadata) {
    validatePayload(payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    String aggregateId = CommonUtils.getAggregateId(payload);
    long timestamp = Instant.now().toEpochMilli();
    String messageId = CommonUtils.createMessageId(aggregateId, timestamp);
    String topic = CommonUtils.getTopicInfo(payload).value();
    String correlationId = UUID.randomUUID().toString();

    Command command = Command.builder()
        .aggregateId(aggregateId)
        .messageId(messageId)
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(Metadata.CORRELATION_ID, correlationId)
            .build())
        .build();

    ProducerRecord<String, Message> record = new ProducerRecord<>(topic, null, timestamp, aggregateId, command);

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), command.getAggregateId());
    producer.send(record);
  }

  @Override
  public CompletableFuture<Object> send(Object payload, Metadata metadata) {
    validatePayload(payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    String aggregateId = CommonUtils.getAggregateId(payload);
    long timestamp = Instant.now().toEpochMilli();
    String messageId = CommonUtils.createMessageId(aggregateId, timestamp);
    String topic = CommonUtils.getTopicInfo(payload).value();
    String correlationId = UUID.randomUUID().toString();

    Command command = Command.builder()
        .aggregateId(aggregateId)
        .messageId(messageId)
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(Metadata.CORRELATION_ID, correlationId)
            .entry(Metadata.REPLY_TO, applicationId)
            .build())
        .build();

    ProducerRecord<String, Message> record = new ProducerRecord<>(topic, null, timestamp, aggregateId, command);

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), command.getAggregateId());
    producer.send(record);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(correlationId, future);

    return future;
  }

  @Override
  public Consumer<String, Message> getConsumer() {
    consumer.subscribe(Collections.singletonList(applicationId.concat(".results")));
    return consumer;
  }

  @Override
  public void onMessage(ConsumerRecords<String, Message> consumerRecords) {
    consumerRecords.forEach(record -> {
      String correlationId = record.value().getMetadata().get(Metadata.CORRELATION_ID);
      if (StringUtils.isBlank(correlationId)) {
        return;
      }
      // CompletableFuture<Object> future = futures.remove(correlationId);
      CompletableFuture<Object> future = cache.getIfPresent(correlationId);
      if (future != null) {
        Exception exception = checkForErrors(record);
        if (exception == null) {
          future.complete(record.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(correlationId);
      }
    });
  }

  private Exception checkForErrors(ConsumerRecord<String, Message> record) {
    Message message = record.value();
    Metadata metadata = message.getMetadata();

    if (metadata.get(Metadata.RESULT).equals("failure")) {
      return new CommandExecutionException(metadata.get(Metadata.CAUSE));
    }

    return null;
  }
}
