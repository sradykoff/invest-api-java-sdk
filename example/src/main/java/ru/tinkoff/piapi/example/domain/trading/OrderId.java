package ru.tinkoff.piapi.example.domain.trading;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class OrderId {
  private final String id;
}
