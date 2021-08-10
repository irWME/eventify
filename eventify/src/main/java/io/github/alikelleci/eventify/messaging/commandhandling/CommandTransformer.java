package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Optional;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private ProcessorContext context;
  private TimestampedKeyValueStore<String, Aggregate> snapshotStore;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = Handlers.COMMAND_HANDLERS.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    // 1. Load aggregate state
    Aggregate aggregate = Optional.ofNullable(snapshotStore.get(key))
        .map(ValueAndTimestamp::value)
        .orElse(null);

    // 2. Validate command against aggregate
    return commandHandler.apply(aggregate, command);
  }

  @Override
  public void close() {

  }

}
