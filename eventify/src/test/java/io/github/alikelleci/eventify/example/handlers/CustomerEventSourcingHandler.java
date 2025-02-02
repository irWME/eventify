package io.github.alikelleci.eventify.example.handlers;

import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerEventSourcingHandler {

  @ApplyEvent
  public Customer handle(CustomerCreated event, Customer state, Metadata metadata) {
    return Customer.builder()
        .id(event.getId())
        .firstName(event.getFirstName())
        .lastName(event.getLastName())
        .credits(event.getCredits())
        .birthday(event.getBirthday())
        .dateCreated(metadata.getTimestamp())
        .build();
  }

  @ApplyEvent
  public Customer handle(FirstNameChanged event, Customer state) {
    return state.toBuilder()
        .firstName(event.getFirstName())
        .build();
  }

  @ApplyEvent
  public Customer handle(LastNameChanged event, Customer state) {
    return state.toBuilder()
        .lastName(event.getLastName())
        .build();
  }

  @ApplyEvent
  public Customer handle(CreditsAdded event, Customer state) {
    return state.toBuilder()
        .credits(state.getCredits() + event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(CreditsIssued event, Customer state) {
    return state.toBuilder()
        .credits(state.getCredits() - event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(CustomerDeleted event, Customer state) {
    return null;
  }
}
