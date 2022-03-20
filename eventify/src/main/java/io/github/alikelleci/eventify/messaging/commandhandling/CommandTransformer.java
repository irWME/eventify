package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.Repository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;
import java.util.Optional;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private final Map<Class<?>, CommandHandler> commandHandlers;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;
  private final boolean deleteEventsOnSnapshot;

  private Repository repository;

  public CommandTransformer(Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers, boolean deleteEventsOnSnapshot) {
    commandHandlers = commandHandlers;
    eventSourcingHandlers = eventSourcingHandlers;
    this.deleteEventsOnSnapshot = deleteEventsOnSnapshot;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext, eventSourcingHandlers);
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = commandHandlers.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    // 1. Load aggregate state
    Aggregate aggregate = repository.loadAggregate(key);

    // 2. Validate command against aggregate
    CommandResult result = commandHandler.apply(command, aggregate);

    if (result instanceof Success) {
      // 3. Save events
      ((Success) result).getEvents().forEach(event ->
          repository.saveEvent(event));

      // 4. Save snapshot if needed
      Optional.ofNullable(aggregate)
          .filter(aggr -> aggr.getSnapshotTreshold() > 0)
          .filter(aggr -> aggr.getVersion() % aggr.getSnapshotTreshold() == 0)
          .ifPresent(aggr -> {
            log.debug("Creating snapshot: {}", aggr);
            repository.saveSnapshot(aggr);

            // 5. Delete events after snapshot
            if (deleteEventsOnSnapshot) {
              log.debug("Events prior to this snapshot will be deleted");
              repository.deleteEvents(aggr);
            }
          });
    }

    return result;
  }

  @Override
  public void close() {

  }
}
