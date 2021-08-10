package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class EventSourcingStream {

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> events = builder.stream(Topics.EVENTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null);

    // Events --> Snapshots
    KStream<String, Aggregate> snapshots = events
        .transformValues(EventSourcingTransformer::new, "snapshot-store")
        .filter((key, aggregate) -> aggregate != null);

    // Snapshots Push
    snapshots
        .to((key, aggregate, recordContext) -> CommonUtils.getTopicInfo(aggregate.getPayload()).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));
  }

}
