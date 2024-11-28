package ru.tinkoff.piapi.example.domain.portfolio;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Either;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import ru.tinkoff.piapi.core.models.Quantity;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

@RequiredArgsConstructor
@Data
public class Portfolio {

  final Map<PositionId, Position> positions;

  public Portfolio withPositionQuantity(PositionId positionId, Quantity quantity) {
    return new Portfolio(positions.put(positionId, new Position(positionId, BlockedAmount.of(quantity))));
  }

  public Either<NoBalanceAvailable, Tuple2<Portfolio, BlockedAmountEvent>> block(PositionId positionId, Quantity quantity) {
    var event = new BlockedAmountEvent(positionId, quantity);
    return applyEvent(event)
      .map(portfolio -> Tuple.of(portfolio, event));
  }

  public Either<NoBalanceAvailable, Tuple2<Portfolio, UnblockedAmountEvent>> unblock(PositionId positionId, Quantity quantity) {
    var event = new UnblockedAmountEvent(positionId, quantity);
    return applyEvent(event)
      .map(portfolio -> Tuple.of(portfolio, event));
  }

  public Either<NoBalanceAvailable, Tuple2<Portfolio, PositionAmountAdded>> addQuantity(PositionId positionId, Quantity quantity) {
    var event = new PositionAmountAdded(positionId, quantity);
    return applyEvent(event)
      .map(portfolio -> Tuple.of(portfolio, event));
  }

  public Either<NoBalanceAvailable, Tuple2<Portfolio, PositionAmountWithdrawnEvent>> blockedWithdraw(PositionId positionId, Quantity quantity) {
    var event = new PositionAmountWithdrawnEvent(positionId, quantity);
    return applyEvent(event)
      .map(portfolio -> Tuple.of(portfolio, event));
  }

  public Either<NoBalanceAvailable, Portfolio> applyEvent(OperationEvent event) {
    switch (event.type) {
      case POSITION_AMOUNT_ADDED:
        var computed = positions
          .computeIfAbsent(event.positionId, id -> new Position(id, BlockedAmount.zero()))
          ._2
          .computeIfPresent(
            event.positionId,
            (id, position) -> position.mapAmount(amount -> amount.add(event.quantity)));
        return Either.right(new Portfolio(computed._2));
      case POSITION_AMOUNT_WITHDRAWN:
        return positions.get(event.positionId)
          .map(position -> position.mapAmountEither(
            amount -> amount.withdrawBlocked(event.quantity)
          ))
          .map(positionEither -> positionEither
            .map(position -> positions.put(event.positionId, position))
            .map(Portfolio::new)
          )
          .getOrElse(() -> Either.left(new NoBalanceAvailable("Position not found")));
      case BLOCKED_AMOUNT_CHANGED:
        return positions.get(event.positionId)
          .map(position -> position.mapAmountEither(
            amount -> amount.block(event.quantity)
          ))
          .map(positionEither -> positionEither
            .map(position -> positions.put(event.positionId, position))
            .map(Portfolio::new)
          )
          .getOrElse(() -> Either.left(new NoBalanceAvailable("Position not found")));
      case UNBLOCKED_AMOUNT_CHANGED:
        return positions.get(event.positionId)
          .map(position -> position.mapAmountEither(
            amount -> amount.unblock(event.quantity)
          ))
          .map(positionEither -> positionEither
            .map(position -> positions.put(event.positionId, position))
            .map(Portfolio::new)
          )
          .getOrElse(() -> Either.left(new NoBalanceAvailable("Position not found")));
      default:
        return Either.left(new NoBalanceAvailable("unknown type of event"));
    }
  }

  @RequiredArgsConstructor
  @Data
  public static class Position {
    final PositionId positionId;
    final BlockedAmount amount;

    Position mapAmount(Function<BlockedAmount, BlockedAmount> f) {
      return new Position(positionId, f.apply(amount));
    }

    Either<NoBalanceAvailable, Position> mapAmountEither(Function<BlockedAmount, Either<NoBalanceAvailable, BlockedAmount>> f) {
      return f.apply(amount).map(nextAmount -> this.mapAmount(__ -> nextAmount));
    }
  }


  public static class BlockedAmountEvent extends OperationEvent {
    public BlockedAmountEvent(PositionId positionId, Quantity quantity) {
      super(positionId, OperationEventType.BLOCKED_AMOUNT_CHANGED, quantity);
    }
  }

  public static class UnblockedAmountEvent extends OperationEvent {
    public UnblockedAmountEvent(PositionId positionId, Quantity quantity) {
      super(positionId, OperationEventType.UNBLOCKED_AMOUNT_CHANGED, quantity);
    }
  }

  public static class PositionAmountWithdrawnEvent extends OperationEvent {
    public PositionAmountWithdrawnEvent(PositionId positionId, Quantity quantity) {
      super(positionId, OperationEventType.POSITION_AMOUNT_WITHDRAWN, quantity);
    }
  }

  public static class PositionAmountAdded extends OperationEvent {
    public PositionAmountAdded(PositionId positionId, Quantity quantity) {
      super(positionId, OperationEventType.POSITION_AMOUNT_ADDED, quantity);
    }
  }


  @RequiredArgsConstructor
  @Data
  public abstract static class OperationEvent {
    final UUID eventId;
    final Instant eventTime;
    final PositionId positionId;
    final OperationEventType type;
    final Quantity quantity;

    public OperationEvent(PositionId positionId, OperationEventType type, Quantity quantity) {
      this(UUID.randomUUID(), Instant.now(), positionId, type, quantity);
    }


  }

  public enum OperationEventType {
    BLOCKED_AMOUNT_CHANGED,
    UNBLOCKED_AMOUNT_CHANGED,
    POSITION_AMOUNT_ADDED,
    POSITION_AMOUNT_WITHDRAWN
  }

}
