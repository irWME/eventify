package io.github.alikelleci.eventify.messaging;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface StreamApp {

  Topology topology();

  Properties properties();

  void start();

  void stop();
}
