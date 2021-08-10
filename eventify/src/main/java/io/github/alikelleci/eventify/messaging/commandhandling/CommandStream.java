package io.github.alikelleci.eventify.messaging.commandhandling;


import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.StreamApp;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingTransformer;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Properties;

@Slf4j
public class CommandStream implements StreamApp {
  private final String applicationId;
  private final String bootstrapServers;
  private final String securityProtocol;
  private final Properties streamsConfig;

  private KafkaStreams.StateListener stateListener;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

  private KafkaStreams kafkaStreams;

  public CommandStream(String applicationId,
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

    // Event store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("event-store"), Serdes.String(), CustomSerdes.Json(Event.class))
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshot-store"), Serdes.String(), CustomSerdes.Json(Aggregate.class))
        .withLoggingEnabled(Collections.emptyMap()));

    // --> Commands
    KStream<String, Command> commands = builder.stream(Topics.COMMANDS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(CommandTransformer::new, "event-store", "snapshot-store")
        .filter((key, result) -> result != null);

    // Results --> Push
    commandResults
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command.getPayload()).value().concat(".results"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

    // Events --> Push
    commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null)
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event.getPayload()).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

    // --> Events
    KStream<String, Event> events = builder.stream(Topics.EVENTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null);

    // Events --> Event Store
    events
        .transformValues(EventSourcingTransformer::new, "event-store", "snapshot-store");

    return builder.build();
  }

  @Override
  public Properties properties() {
    Properties properties = new Properties();
    streamsConfig.forEach(properties::put);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-command-handler");

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

