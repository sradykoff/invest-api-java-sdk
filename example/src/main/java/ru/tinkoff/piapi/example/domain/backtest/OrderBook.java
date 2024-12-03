package ru.tinkoff.piapi.example.domain.backtest;

import io.vavr.Function1;
import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Queue;
import io.vavr.collection.Set;
import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.With;
import ru.tinkoff.piapi.contract.v1.OrderDirection;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.trading.OrderId;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

@With
@RequiredArgsConstructor
public class OrderBook {

  private final InstrumentId instrumentId;
  private final SortedMap<BigDecimal, Queue<Order>> sellOrders;
  private final SortedMap<BigDecimal, Queue<Order>> buyOrders;
  private final Set<OrderId> orderIds;

  public OrderBook(InstrumentId instrumentId) {
    this(instrumentId, TreeMap.empty(), TreeMap.empty(Comparator.reverseOrder()), HashSet.empty());
  }

  public OrderBook addOrder(OrderId id, Quantity quantity, Quantity price, OrderSide side) {
    var order = new Order(id, side, quantity, price);
    return modifyOrdersMap(side, orderMapParam -> orderMapParam
      .computeIfPresent(price.getValue(), (cpKey, queue) -> queue.enqueue(order))
      .apply((existsQueue, nextMap) -> existsQueue
        .map(__ -> (SortedMap<BigDecimal, Queue<Order>>) nextMap)
        .getOrElse(() -> nextMap.computeIfAbsent(price.getValue(), cpKey -> Queue.of(order))._2)
      )
    );
  }

  Tuple2<SortedMap<BigDecimal, Queue<Order>>, OrderBookExecutionEvent> runExecution(SortedMap<BigDecimal, Queue<Order>> orderMap, Order order) {
    OrderBookExecutionEvent event = new OrderBookExecutionEvent(instrumentId, List.empty());

    AtomicReference<Quantity> quantityVolume = new AtomicReference<>(Quantity.ZERO);
    var volumeOrdersMap = orderMap
      .filter((comparingPrice, orderQueue) -> order.isExecutablePrice(comparingPrice))
      .toStream()
      .map(orderTuple -> orderTuple.apply((comparingPrice, orderQueue) -> Tuple.of(
        comparingPrice,
        orderQueue.takeWhile(
            frontOrder -> quantityVolume.updateAndGet(q -> q.add(frontOrder.quantity)).isLessOrEquals(order.quantity)
          )
          .map(comparingOrder -> Tuple.<Order, Function2<SortedMap<BigDecimal, Queue<Order>>, Order, SortedMap<BigDecimal, Queue<Order>>>>of(
            comparingOrder,
            (nextOrderMap, nextOrder) -> nextOrderMap.computeIfPresent(
              comparingPrice,
              (cpKey, paramOrderQueue) -> paramOrderQueue.replace(comparingOrder, nextOrder)
            )._2
          ))
      )));

    if (volumeOrdersMap.isEmpty()) return Tuple.of(orderMap, event);

    var frontOrdersFilled = volumeOrdersMap.toStream()
      .map(Tuple2::_2)
      .flatMap(Queue::toStream)
      .map(frontOrderTuple -> frontOrderTuple
        .append(frontOrderTuple._1.fill(order)));


    var currentOrderState = Tuple.of(order, List.<OrderFilledEvent>empty());
    var currentFilledOrder = frontOrdersFilled
      .map(Tuple3::_3)
      .foldRight(currentOrderState, (frontEvent, currentOrderTuple) -> currentOrderTuple.apply((currentOrder, events) -> {
        var currentFill = currentOrder.fill(frontEvent._2);
        return currentFill.apply((nextOrder, currentEvent) -> Tuple.of(nextOrder, events.prepend(currentEvent)));
      }));

    var nextOrderMap = frontOrdersFilled
      .foldRight(orderMap, (frontFilledTuple, nextMapParam) -> frontFilledTuple._2.apply(nextMapParam, frontFilledTuple._1));
    var nextEvent = frontOrdersFilled
      .map(Tuple3::_3)
      .map(Tuple2::_2)
      .foldRight(event, (frontFilledEvent, nextEventParam) -> nextEventParam.withOrderEvents(nextEventParam.orderEvents.append(frontFilledEvent)));

    nextEvent = nextEvent.withOrderEvents(nextEvent.orderEvents.appendAll(currentFilledOrder._2));

    return Tuple.of(nextOrderMap, nextEvent);
  }

  public SortedMap<BigDecimal, Queue<Order>> getOrdersMap(OrderSide side) {
    return getOrdersMap(side, true);
  }

  SortedMap<BigDecimal, Queue<Order>> getOrdersMap(OrderSide side, boolean isSameSide) {
    if (!isSameSide) side = (side == OrderSide.BID) ? OrderSide.ASK : OrderSide.BID;
    switch (side) {
      case BID:
        return buyOrders;
      case ASK:
        return sellOrders;
      default:
        return TreeMap.empty();
    }
  }

  OrderBook modifyOrdersMap(OrderSide side, Function1<SortedMap<BigDecimal, Queue<Order>>, SortedMap<BigDecimal, Queue<Order>>> func) {
    if (side == OrderSide.BID) {
      return this.withBuyOrders(func.apply(this.buyOrders));
    } else {
      return this.withSellOrders(func.apply(this.sellOrders));
    }
  }

  public enum OrderSide {
    BID, //buy
    ASK, //sell
  }

  @RequiredArgsConstructor
  @Getter
  @With
  public static class OrderBookExecutionEvent {
    final UUID eventId = UUID.randomUUID();
    private final InstrumentId instrumentId;
    private final List<OrderFilledEvent> orderEvents;
  }


  @Getter
  @With
  @RequiredArgsConstructor
  public static class Order {
    final OrderId orderId;
    final OrderSide side;
    final Quantity quantity;
    final Quantity price;

    public boolean isExecutable() {
      return quantity.isGreater(Quantity.ZERO);
    }

    boolean isExecutablePrice(BigDecimal comparingPrice) {
      if (isExecutable()) {
        if (side == OrderSide.BID) {
          return price.isGreaterOrEquals(new Quantity(comparingPrice));
        } else {
          return price.isLessOrEquals(new Quantity(comparingPrice));
        }
      }
      return false;
    }

    Tuple2<Order, OrderFilledEvent> fill(Order asideOrder) {
      if (asideOrder.quantity.isGreaterOrEquals(this.quantity)) {
        return Tuple.of(this.withQuantity(Quantity.ZERO), new OrderFilledEvent(orderId, side, asideOrder.quantity, asideOrder.price, true));
      } else {
        return Tuple.of(this.withQuantity(this.quantity.subtract(quantity)), new OrderFilledEvent(orderId, side, quantity, asideOrder.price, false));
      }
    }

    Tuple2<Order, OrderFilledEvent> fill(OrderFilledEvent asideOrder) {
      if (asideOrder.quantity.isGreaterOrEquals(this.quantity)) {
        return Tuple.of(this.withQuantity(Quantity.ZERO), new OrderFilledEvent(orderId, side, asideOrder.quantity, asideOrder.price, true));
      } else {
        return Tuple.of(this.withQuantity(this.quantity.subtract(quantity)), new OrderFilledEvent(orderId, side, quantity, asideOrder.price, false));
      }
    }
  }


  @RequiredArgsConstructor
  @Getter
  public static class OrderFilledEvent {
    final UUID eventId = UUID.randomUUID();
    final OrderId orderId;
    final OrderSide side;
    final Quantity quantity;
    final Quantity price;
    final boolean fullFilled;

    public boolean isNotEmpty() {
      return quantity.isGreater(Quantity.ZERO);
    }
  }
}
