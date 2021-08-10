package io.github.alikelleci.eventify.messaging.eventhandling;


import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.StreamApp;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class EventStream implements StreamApp {
  private final String applicationId;
  private final String bootstrapServers;
  private final String securityProtocol;
  private final Properties streamsConfig;

  private KafkaStreams.StateListener stateListener;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

  private KafkaStreams kafkaStreams;

  public EventStream(String applicationId,
                     String bootstrapServers,
                     String securityProtocol, Properties streamsConfig,
                     KafkaStreams.StateListener stateListener,
                     Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.applicationId = applicationId;
    this.bootstrapServers = bootstrapServers;
    this.securityProtocol = securityProtocol;
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  @Override
  public Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    // --> Events
    KStream<String, Event> events = builder.stream(Topics.EVENTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null);

    // Events --> Void
    events
        .transformValues(EventTransformer::new);

    return builder.build();
  }

  @Override
  public Properties properties() {
    Properties properties = new Properties();
    streamsConfig.forEach(properties::put);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-event-handler");

    return properties;
  }

  @Override
  public void start() {
    kafkaStreams = new KafkaStreams(topology(), properties());
    kafkaStreams.setStateListener(stateListener);
    kafkaStreams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    kafkaStreams.start();
  }

  @Override
  public void stop() {
    kafkaStreams.close();
  }

}
