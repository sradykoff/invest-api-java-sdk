package ru.tinkoff.piapi.example.domain.portfolio;

public class NoBalanceAvailable extends RuntimeException {
  public NoBalanceAvailable(String message) {
    super(message);
  }
}
