package ru.tinkoff.piapi.example.domain.portfolio;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.InstrumentId;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

@RequiredArgsConstructor
@Data
public class Portfolio {

  final Map<PositionId, Position> positions;
  final List<PendingOperation> pendingOperations;
  final List<OperationEvent> events;


  public Portfolio withPositionQuantity(PositionId positionId, Quantity quantity) {
    return new Portfolio(positions.put(positionId, new Position(positionId, BlockedAmount.of(quantity))), pendingOperations, this.events);
  }

  public Portfolio block(PositionId positionId, Quantity quantity) {
    var computed = positions.computeIfPresent(positionId, (id, position) -> new Position(id, position.amount.block(quantity)
      .getOrElseThrow(Function.identity())));
    return new Portfolio(computed._2, pendingOperations, this.events);
  }

  public Portfolio unblock(PositionId positionId, Quantity quantity) {
    var computed = positions.computeIfPresent(positionId, (id, position) -> new Position(id, position.amount.unblock(quantity)
      .getOrElseThrow(Function.identity())));
    return new Portfolio(computed._2, pendingOperations, this.events);
  }

  public Portfolio addQuantity(PositionId positionId, Quantity quantity) {
    var computed = positions.computeIfPresent(positionId, (id, position) -> new Position(id, position.amount.add(quantity)));
    return new Portfolio(computed._2, pendingOperations, this.events);
  }

  public Portfolio blockedWithdraw(PositionId positionId, Quantity quantity) {
    var computed = positions.computeIfPresent(positionId, (id, position) -> new Position(id, position.amount.withdrawBlocked(quantity)
      .getOrElseThrow(Function.identity())));
    return new Portfolio(computed._2, pendingOperations, this.events);
  }


  @RequiredArgsConstructor
  @Data
  public static class Position {
    final PositionId positionId;
    final BlockedAmount amount;
  }

  @RequiredArgsConstructor
  @Data
  public static class PendingOperation {
    private final UUID id;
    private final Position position;
    private final OperationDirection direction;
    private final Quantity quantity;
    private final Quantity price;
  }


  public enum OperationDirection {
    BUY, SELL
  }


  @RequiredArgsConstructor
  @Data
  public static class OperationEvent {
    final UUID eventId;
    final Instant eventTime;
    final PositionId positionId;
    final OperationEventType type;
    final Quantity quantity;
    final Position position;
  }

  public enum OperationEventType {
    BUY, SELL, TAX, OTHER,
  }

}
