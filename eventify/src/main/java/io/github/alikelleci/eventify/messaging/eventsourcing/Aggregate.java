package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshots;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {
  private String aggregateId;
  private String eventId;
  private long version;

  protected Aggregate() {
    this.aggregateId = null;
    this.eventId = null;
    this.version = 0;
  }

  @Builder
  protected Aggregate(Instant timestamp, Object payload, Metadata metadata, String eventId, long version) {
    super(timestamp, payload, metadata);

    this.aggregateId = Optional.ofNullable(payload)
        .flatMap(p -> FieldUtils.getFieldsListWithAnnotation(p.getClass(), AggregateId.class).stream()
            .filter(field -> field.getType() == String.class)
            .findFirst()
            .map(field -> {
              field.setAccessible(true);
              return (String) ReflectionUtils.getField(field, p);
            }))
        .orElse(null);

    this.eventId = eventId;
    this.version = version;
  }

  @Transient
  public int getSnapshotTreshold() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, EnableSnapshots.class))
        .map(EnableSnapshots::threshold)
        .filter(threshold -> threshold > 0)
        .orElse(0);
  }


}
