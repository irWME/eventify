package io.github.alikelleci.eventify.messaging.upcasting.exceptions;

public class UpcastingException extends RuntimeException {

  public UpcastingException(String message) {
    super(message);
  }

  public UpcastingException(String message, Throwable cause) {
    super(message, cause);
  }
}
