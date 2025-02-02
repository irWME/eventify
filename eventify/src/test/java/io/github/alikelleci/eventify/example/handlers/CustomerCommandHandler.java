package io.github.alikelleci.eventify.example.handlers;

import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.AddCredits;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.ChangeFirstName;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.ChangeLastName;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.DeleteCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.IssueCredits;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import lombok.extern.slf4j.Slf4j;

import javax.validation.ValidationException;


@Slf4j
public class CustomerCommandHandler {

  @HandleCommand
  public CustomerEvent handle(CreateCustomer command, Customer state, Metadata metadata) {
    if (state != null) {
      throw new ValidationException("Customer already exists.");
    }

    return CustomerCreated.builder()
        .id(command.getId())
        .firstName(command.getFirstName())
        .lastName(command.getLastName())
        .credits(command.getCredits())
        .birthday(command.getBirthday())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(ChangeFirstName command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return FirstNameChanged.builder()
        .id(command.getId())
        .firstName(command.getFirstName())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(ChangeLastName command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return LastNameChanged.builder()
        .id(command.getId())
        .lastName(command.getLastName())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(AddCredits command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return CreditsAdded.builder()
        .id(command.getId())
        .amount(command.getAmount())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(IssueCredits command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    if (state.getCredits() < command.getAmount()) {
      throw new ValidationException("Credits not issued: not enough credits available.");
    }

    return CreditsIssued.builder()
        .id(command.getId())
        .amount(command.getAmount())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(DeleteCustomer command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return CustomerDeleted.builder()
        .id(command.getId())
        .build();
  }
}
