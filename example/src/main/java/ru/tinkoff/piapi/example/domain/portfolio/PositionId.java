package ru.tinkoff.piapi.example.domain.portfolio;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import ru.tinkoff.piapi.example.domain.InstrumentId;

@RequiredArgsConstructor
@Data
public class PositionId {
  private final String id;
  private final InstrumentId instrumentId;
}
