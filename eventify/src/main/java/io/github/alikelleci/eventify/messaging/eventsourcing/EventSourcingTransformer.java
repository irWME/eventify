package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Optional;

@Slf4j
public class EventSourcingTransformer implements ValueTransformerWithKey<String, Event, Aggregate> {

  private ProcessorContext context;
  private TimestampedKeyValueStore<String, Aggregate> snapshotStore;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public Aggregate transform(String key, Event event) {
    EventSourcingHandler eventSourcingHandler = Handlers.EVENTSOURCING_HANDLERS.get(event.getPayload().getClass());
    if (eventSourcingHandler == null) {
      return null;
    }

    Aggregate aggregate = Optional.ofNullable(snapshotStore.get(key))
        .map(ValueAndTimestamp::value)
        .orElse(null);

    aggregate = eventSourcingHandler.apply(aggregate, event);

    if (aggregate != null) {
      snapshotStore.put(key, ValueAndTimestamp.make(aggregate, context.timestamp()));
    }

    return aggregate;
  }

  @Override
  public void close() {

  }

}
