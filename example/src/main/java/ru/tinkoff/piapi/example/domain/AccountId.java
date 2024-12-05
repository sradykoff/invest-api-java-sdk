package ru.tinkoff.piapi.example.domain;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class AccountId {
  private final String id;
}
