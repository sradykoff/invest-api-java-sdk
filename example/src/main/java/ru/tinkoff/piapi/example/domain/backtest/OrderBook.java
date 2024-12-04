package ru.tinkoff.piapi.example.domain.backtest;

import io.vavr.Function1;
import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Queue;
import io.vavr.collection.SortedMap;
import io.vavr.collection.Stream;
import io.vavr.collection.TreeMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.With;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.trading.OrderId;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@With
@RequiredArgsConstructor
public class OrderBook {

  private final InstrumentId instrumentId;
  private final SortedMap<BigDecimal, Queue<Order>> sellOrders;
  private final SortedMap<BigDecimal, Queue<Order>> buyOrders;
  private final Map<OrderId, Order> orders;

  public OrderBook(InstrumentId instrumentId) {
    this(instrumentId, TreeMap.empty(), TreeMap.empty(Comparator.reverseOrder()), HashMap.empty());
  }

  public Tuple2<OrderBook, OrderBookExecutionEvent> addOrder(OrderId id, Quantity quantity, Quantity price, OrderSide side) {
    OrderBookExecutionEvent event = new OrderBookExecutionEvent(instrumentId, List.of(new OrderAddedEvent(id, side, quantity, price)));
    var order = new Order(id, side, quantity, price);

    var asideSide = getOrderSide(side, false);
    var asideMap = getOrdersMap(asideSide);
    var executionTuple = runExecution(asideMap, order);


    return executionTuple.apply((nextASideMap, executionEvent) -> {

      var filledEventOpt = executionEvent.getOrderEvents()
        .filter(orderBookEvent -> orderBookEvent instanceof OrderFilledEvent)
        .map(orderBookEvent -> (OrderFilledEvent) orderBookEvent);

      if (!filledEventOpt.isEmpty()) {
        var totalVolume = filledEventOpt.map(q -> q.quantity)
          .foldRight(Quantity.ZERO, Quantity::add);
        if (totalVolume.isGreaterOrEquals(order.quantity)) {
          return Tuple.of(
            modifyOrdersMap(asideSide, __ -> nextASideMap
              .mapValues(queue -> queue.filter(Order::isExecutable))
              .filterValues(queue -> !queue.isEmpty())),
            executionEvent.withOrderEvents(executionEvent.getOrderEvents()
              .appendAll(event.orderEvents))
          );
        } else {
          return Tuple.of(
            modifyOrdersMap(asideSide, __ -> nextASideMap
              .mapValues(queue -> queue.filter(Order::isExecutable))
              .filterValues(queue -> !queue.isEmpty()))
              .addOrderToMap(order.withQuantity(order.quantity
                .subtract(filledEventOpt.map(q -> q.quantity)
                  .foldRight(Quantity.ZERO, Quantity::add)))),
            executionEvent.withOrderEvents(executionEvent.getOrderEvents()
              .appendAll(event.orderEvents))
          );
        }

      }

      return Tuple.of(addOrderToMap(order), event);

    })

      ;
  }


  public Tuple2<OrderBook, OrderBookExecutionEvent> removeOrder(OrderId id) {
    OrderBookExecutionEvent event = new OrderBookExecutionEvent(instrumentId, List.empty());
    var existsOrderOpt = findOrderById(id);
    if (existsOrderOpt.isPresent()) {
      var order = existsOrderOpt.get();
      return Tuple.of(
        modifyOrdersMap(
          this.withOrders(this.orders.remove(id)),
          order.side,
          orderMapParam -> orderMapParam
            .computeIfPresent(order.price.getValue(), (cpKey, queue) -> queue.filter(o -> !o.orderId.equals(id)))
            ._2
            .filterValues(q -> !q.isEmpty())
        ),
        event.withOrderEvents(List.of(
          new OrderRemovedEvent(id, order.side, order.quantity, order.price)
        )));
    }
    return Tuple.of(this, event.withOrderEvents(List.of(new OrderNotFoundEvent(id))));
  }

  public Optional<Order> findOrderById(OrderId id) {
    return orders.get(id).toJavaOptional();
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
            frontOrder -> quantityVolume.getAndUpdate(q -> q.add(frontOrder.quantity)).isLessOrEquals(order.quantity)
          )
          .map(comparingOrder -> Tuple.<Order, OrderMapModifier>of(
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
      .foldRight(Tuple.of(
          order,
          Stream.<Tuple3<Order, OrderMapModifier, Tuple2<Order, OrderFilledEvent>>>empty()
        ),
        (frontOrderTuple, filledOrders) -> frontOrderTuple._1.fill(filledOrders._1)
          .apply((nextOrder, orderEvent) -> Tuple.of(filledOrders._1.withQuantity(filledOrders._1.quantity.subtract(orderEvent.quantity)), filledOrders._2.append(frontOrderTuple.append(Tuple.of(nextOrder, orderEvent))))))
      //.map2(evt -> filledOrders._2.append(frontOrderTuple)))
      ._2;


    var currentOrderState = Tuple.of(order, List.<OrderFilledEvent>empty());
    var currentFilledOrder = frontOrdersFilled
      .map(Tuple3::_3)
      .foldRight(currentOrderState, (frontEvent, currentOrderTuple) -> currentOrderTuple.apply((currentOrder, events) -> {
        var currentFill = currentOrder.fill(frontEvent._1);
        return currentFill.apply((nextOrder, currentEvent) -> Tuple.of(nextOrder, events.prepend(currentEvent)));
      }));

    var nextOrderMap = frontOrdersFilled
      .foldRight(orderMap, (frontFilledTuple, nextMapParam) -> frontFilledTuple._2.apply(nextMapParam, frontFilledTuple._3._1))
      .mapValues(queue -> queue.filter(Order::isExecutable))
      .filterValues(q -> !q.isEmpty());
    var nextEvent = frontOrdersFilled
      .map(Tuple3::_3)
      .map(Tuple2::_2)
      .foldRight(event, (frontFilledEvent, nextEventParam) -> nextEventParam.withOrderEvents(nextEventParam.orderEvents.append(frontFilledEvent)));

    nextEvent = nextEvent.withOrderEvents(nextEvent.orderEvents.appendAll(currentFilledOrder._2));

    return Tuple.of(nextOrderMap, nextEvent);
  }


  public SortedMap<BigDecimal, Queue<Order>> getOrdersMap(OrderSide side) {
    switch (side) {
      case BID:
        return buyOrders;
      case ASK:
        return sellOrders;
      default:
        return TreeMap.empty();
    }
  }

  OrderSide getOrderSide(OrderSide side, boolean isSameSide) {
    if (!isSameSide) side = (side == OrderSide.BID) ? OrderSide.ASK : OrderSide.BID;
    return side;
  }

  OrderBook modifyOrdersMap(OrderSide side, Function1<SortedMap<BigDecimal, Queue<Order>>, SortedMap<BigDecimal, Queue<Order>>> func) {
    return modifyOrdersMap(this, side, func);
  }

  OrderBook addOrderToMap(Order order) {
    if (!order.isExecutable()) return this;
    return modifyOrdersMap(order.side, orderMapParam -> orderMapParam
      .computeIfPresent(order.price.getValue(), (cpKey, queue) -> queue.enqueue(order))
      .apply((existsQueue, nextMap) -> existsQueue
        .map(__ -> (SortedMap<BigDecimal, Queue<Order>>) nextMap)
        .getOrElse(() -> nextMap.computeIfAbsent(order.price.getValue(), cpKey -> Queue.of(order))._2)
      )
    ).withOrders(this.orders.put(order.orderId, order));
  }

  static OrderBook modifyOrdersMap(OrderBook orderBook, OrderSide side, Function1<SortedMap<BigDecimal, Queue<Order>>, SortedMap<BigDecimal, Queue<Order>>> func) {
    if (side == OrderSide.BID) {
      return orderBook.withBuyOrders(func.apply(orderBook.buyOrders));
    } else {
      return orderBook.withSellOrders(func.apply(orderBook.sellOrders));
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
    private final List<OrderBookEvent> orderEvents;


  }


  @Getter
  @With
  @ToString
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
        return Tuple.of(
          this.withQuantity(Quantity.ZERO),
          new OrderFilledEvent(
            orderId,
            side,
            this.quantity,
            asideOrder,
            asideOrder.withQuantity(asideOrder.quantity.subtract(this.quantity))
          ));
      } else {
        return Tuple.of(
          this.withQuantity(this.quantity.subtract(asideOrder.quantity)),
          new OrderFilledEvent(
            orderId,
            side,
            asideOrder.quantity,
            asideOrder,
            asideOrder.withQuantity(Quantity.ZERO)
          )
        );
      }
    }

  }

  @Getter
  public static abstract class OrderBookEvent {
    final UUID eventId = UUID.randomUUID();
  }


  @RequiredArgsConstructor
  @Getter
  public static class OrderAddedEvent extends OrderBookEvent {

    final OrderId orderId;
    final OrderSide side;
    final Quantity quantity;
    final Quantity price;

  }

  @RequiredArgsConstructor
  @Getter
  public static class OrderRemovedEvent extends OrderBookEvent {

    final OrderId orderId;
    final OrderSide side;
    final Quantity quantity;
    final Quantity price;

  }

  @RequiredArgsConstructor
  @Getter
  public static class OrderNotFoundEvent extends OrderBookEvent {

    final OrderId orderId;

  }


  @RequiredArgsConstructor
  @Getter
  public static class OrderFilledEvent extends OrderBookEvent {

    final OrderId orderId;
    final OrderSide originSide;
    final Quantity quantity;

    final Order matchedAsideOrder;
    final Order nextAsideOrder;

  }

  public interface OrderMapModifier extends Function2<SortedMap<BigDecimal, Queue<Order>>, Order, SortedMap<BigDecimal, Queue<Order>>> {

  }
}
