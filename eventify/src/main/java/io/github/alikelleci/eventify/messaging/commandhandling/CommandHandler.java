package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.retry.Retry;
import io.github.alikelleci.eventify.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class CommandHandler implements BiFunction<Command, Aggregate, CommandResult> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  public CommandHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling command failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling command failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public CommandResult apply(Command command, Aggregate aggregate) {
    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());

    try {
      validate(command.getPayload());
      return Failsafe.with(retryPolicy).get(() -> doInvoke(command, aggregate));
    } catch (Exception e) {
      Throwable throwable = ExceptionUtils.getRootCause(e);
      String message = ExceptionUtils.getRootCauseMessage(e);

      if (throwable instanceof ValidationException) {
        log.debug("Command rejected: {}", message);
        return Failure.builder()
            .command(command)
            .cause(message)
            .build();
      }
      throw new CommandExecutionException(message, throwable);
    }
  }

  private CommandResult doInvoke(Command command, Aggregate aggregate) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, command.getPayload(), aggregate != null ? aggregate.getPayload() : null);
    } else {
      result = method.invoke(target, command.getPayload(), aggregate != null ? aggregate.getPayload() : null, command.getMetadata());
    }
    return createCommandResult(command, result);
  }

  private CommandResult createCommandResult(Command command, Object result) {
    if (result == null) {
      return null;
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Event> events = list.stream()
        .filter(Objects::nonNull)
        .map(payload -> Event.builder()
            .timestamp(command.getTimestamp())
            .payload(payload)
            .metadata(command.getMetadata())
            .build())
        .collect(Collectors.toList());

    events.forEach(event -> {
      if (event.getPayload() == null) {
        throw new PayloadMissingException("You are trying to dispatch an event without a payload.");
      }
      if (event.getTopicInfo() == null) {
        throw new TopicInfoMissingException("You are trying to dispatch an event without any topic information. Please annotate your event with @TopicInfo.");
      }
      if (event.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to dispatch an event without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
      if (!StringUtils.equals(event.getAggregateId(), command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + command.getAggregateId() + ", but was " + event.getAggregateId());
      }
    });

    return Success.builder()
        .command(command)
        .events(events)
        .build();
  }

  private void validate(Object payload) {
    Set<ConstraintViolation<Object>> violations = validator.validate(payload);
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

  public Method getMethod() {
    return method;
  }
}
