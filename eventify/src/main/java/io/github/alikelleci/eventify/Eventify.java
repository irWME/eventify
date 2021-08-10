package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.StreamApp;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandStream;
import io.github.alikelleci.eventify.messaging.eventhandling.EventStream;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.KafkaStreams;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class Eventify {

  private final String applicationId;
  private final String bootstrapServers;
  private final String securityProtocol;
  private final Properties streamsConfig;

  private KafkaStreams.StateListener stateListener;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

  private final List<StreamApp> streamAppList = new ArrayList<>();

  protected Eventify(String applicationId,
                     String bootstrapServers,
                     String securityProtocol,
                     Properties streamsConfig,
                     KafkaStreams.StateListener stateListener,
                     Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.applicationId = applicationId;
    this.bootstrapServers = bootstrapServers;
    this.securityProtocol = securityProtocol;
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  public void start() {
    if (CollectionUtils.isNotEmpty(Topics.COMMANDS) && CollectionUtils.isNotEmpty(Topics.EVENTS)) {
      streamAppList.add(new CommandStream(
          applicationId,
          bootstrapServers,
          securityProtocol,
          streamsConfig,
          stateListener,
          uncaughtExceptionHandler));
    }

    if (CollectionUtils.isNotEmpty(Topics.EVENTS)) {
      streamAppList.add(new EventStream(
          applicationId,
          bootstrapServers,
          securityProtocol,
          streamsConfig,
          stateListener,
          uncaughtExceptionHandler));
    }

    if (CollectionUtils.isNotEmpty(Topics.RESULTS)) {
      streamAppList.add(new ResultStream(
          applicationId,
          bootstrapServers,
          securityProtocol,
          streamsConfig,
          stateListener,
          uncaughtExceptionHandler));
    }

    streamAppList
        .forEach(StreamApp::start);
  }

  public void stop() {
    streamAppList
        .forEach(StreamApp::stop);
  }

}
