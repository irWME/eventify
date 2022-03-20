package io.github.alikelleci.eventify.experimental.messages;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.Optional;

@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Command extends Message {
  private String aggregateId;

  protected Command(String id, Instant timestamp, Object payload, Metadata metadata) {
    super(id, timestamp, payload, metadata);
    this.aggregateId = createAggregatieId(payload);
  }

  public Command(Instant timestamp, Object payload, Metadata metadata) {
    this(null, timestamp, payload, metadata);
  }

  public Command(Object payload, Metadata metadata) {
    this(null, null, payload, metadata);
  }

  public Command(Object payload) {
    this(null, null, payload, null);
  }


  private String createAggregatieId(Object payload) {
    return Optional.ofNullable(payload).flatMap(p -> FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        }))
        .orElse(null);
  }
}
