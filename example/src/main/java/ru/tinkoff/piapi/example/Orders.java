package ru.tinkoff.piapi.example;

import io.vavr.collection.List;
import io.vavr.control.Either;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.OrderDirection;
import ru.tinkoff.piapi.contract.v1.OrderType;
import ru.tinkoff.piapi.contract.v1.PriceType;
import ru.tinkoff.piapi.contract.v1.TimeInForceType;
import ru.tinkoff.piapi.core.OrdersService;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.core.stream.OrdersStreamService;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class Orders {

  private final OrdersService ordersService;
  private final OrdersStreamService ordersStreamService;


  public IOrdersBox orderBox(Instruments.Instrument instrument, String accountId) {

    var orderBox = new OrdersBox(instrument, accountId);
    var orderStateStreamKey = ordersStreamService.subscribeOrderState(
      orderStateResp -> {
        var orderId = orderStateResp.getOrderState().getOrderId();
        applyState(orderBox.orders, new StatusCommand(UUID.fromString(orderId))
          , state -> state.withExecutionLog(state.getExecutionLog()
            .append(orderStateResp.getOrderState())));
      },
      List.of(accountId)
    );
    return orderBox;
  }

  @RequiredArgsConstructor
  class OrdersBox implements IOrdersBox {
    private final Instruments.Instrument instrument;
    private final String accountId;
    private final ConcurrentMap<UUID, ExecutionStatus> orders = new ConcurrentHashMap<>();

    @Override
    public void execute(OrderCommand command) {

      if (command instanceof LimitOrderCommand) {
        try {
          var call = ordersService.postAsyncOrder(
            instrument.getUuid(),
            ((LimitOrderCommand) command).quantity.getValue().longValue(),
            ((LimitOrderCommand) command).price.toQuotation(),
            ((LimitOrderCommand) command).direction,
            accountId,
            OrderType.ORDER_TYPE_LIMIT,
            ((LimitOrderCommand) command).id.toString(),
            TimeInForceType.TIME_IN_FORCE_DAY,
            PriceType.PRICE_TYPE_UNSPECIFIED
          ).whenComplete((result, error) -> {
            if (error != null) {
              log.error("error post order", error);
              applyState(orders, command, status -> status.withResultCall(Either.right(error)));
            } else {
              applyState(orders, command, status -> status.withResultCall(Either.left(result)));
            }
          });
          applyState(orders, command, status -> (Objects.isNull(status.resultCall))
            ? status.withResultCall(Either.left(call)) : status);
        } catch (Exception error) {
          log.error("error post order", error);
          orders.put(command.id(), new ExecutionStatus(command, Either.right(error), List.empty()));
        }
      }

    }

    @Override
    public ExecutionStatus status(OrderCommand command) {
      return orders.get(command.id());
    }

    @Override
    public Instruments.Instrument instrument() {
      return instrument;
    }
  }

  static void applyState(
    ConcurrentMap<UUID, ExecutionStatus> orders,
    OrderCommand cmd,
    Function<ExecutionStatus, ExecutionStatus> applicator
  ) {
    orders.compute(cmd.id(), (uid, status) -> {
      if (status == null) status = new ExecutionStatus(cmd, null, List.empty());
      return applicator.apply(status);
    });
  }

  public interface IOrdersBox {

    Instruments.Instrument instrument();

    void execute(OrderCommand command);

    ExecutionStatus status(OrderCommand command);
    // waitComplete? or done
    //  CompletableFuture<ExecutionStatus> waitNextStatus();
  }

  @RequiredArgsConstructor
  public static class LimitOrderCommand implements OrderCommand {
    private final UUID id = UUID.randomUUID();
    private final OrderDirection direction;
    private final Quantity quantity;
    private final Quantity price;

    @Override
    public UUID id() {
      return id;
    }

    @Override
    public OrderCmdType type() {
      return OrderCmdType.LIMIT;
    }
  }

  public interface OrderCommand {
    UUID id();

    OrderCmdType type();
  }

  @RequiredArgsConstructor
  public static class StatusCommand implements OrderCommand {
    private final UUID id;

    @Override
    public UUID id() {
      return id;
    }

    @Override
    public OrderCmdType type() {
      return OrderCmdType.STATUS;
    }
  }

  public enum OrderCmdType {
    LIMIT,
    MARKET,
    BESTPRICE,
    CANCEL,
    STATUS

  }


  @Data
  @With
  @RequiredArgsConstructor
  public static class ExecutionStatus {
    private final OrderCommand command;
    private final Either<Object, Throwable> resultCall;
    private final List<Object> executionLog;
  }

}
