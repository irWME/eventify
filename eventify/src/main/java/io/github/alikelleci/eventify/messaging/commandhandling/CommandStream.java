package io.github.alikelleci.eventify.messaging.commandhandling;


import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingTransformer;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class CommandStream {

  public void buildStream(StreamsBuilder builder) {
    // --> Commands
    KStream<String, Command> commands = builder.stream(Topics.COMMANDS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(CommandTransformer::new, "snapshot-store")
        .filter((key, result) -> result != null);

    // Results --> Push
    commandResults
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command.getPayload()).value().concat(".results"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

    // Results --> Events
    KStream<String, Event> events = commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null);

    // Events --> Push
    events
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event.getPayload()).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

    // Events --> Snapshots
    KStream<String, Aggregate> snapshots = events
        .transformValues(EventSourcingTransformer::new, "snapshot-store")
        .filter((key, aggregate) -> aggregate != null);

    // Snapshots --> Push
    snapshots
        .to((key, aggregate, recordContext) -> CommonUtils.getTopicInfo(aggregate.getPayload()).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));

  }

}
