package ru.tinkoff.piapi.example.domain.orderbook;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple5;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.tinkoff.piapi.contract.v1.Order;
import ru.tinkoff.piapi.contract.v1.OrderBook;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.core.utils.DateUtils;
import ru.tinkoff.piapi.example.trading.LiveTradingBot;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.OptionalDouble;

@RequiredArgsConstructor
@Getter
public class OrderBookState {

  static final int MAX_DEPTH = 10;

  final List<OrderBookStats> orderBooks;

  public OrderBookState onOrderBook(OrderBook orderBook) {
    var orderBookTime = ZonedDateTime.ofInstant(DateUtils.timestampToInstant(orderBook.getTime()), ZoneId.of("UTC"));
    return new OrderBookState(
      orderBooks.append(
          new OrderBookStats(
            orderBookTime,
            List.ofAll(orderBook.getAsksList()),
            List.ofAll(orderBook.getBidsList())
          )
        )
        .take(MAX_DEPTH)
    );
  }

  public List<Tuple2<ZonedDateTime, OrderBookStats>> queryLastStats() {
    return orderBooks.map(orderBook -> Tuple.of(
      orderBook.getTime(),
      orderBook
    ));
  }

//  public OptionalDouble averageSellPricePercentChange() {
//    var stats = queryLastStats();
//    if (stats.isEmpty()) return OptionalDouble.empty();
//    var head = stats.head();
//    return stats
//      .dropRight(1)
//      .map(t -> t._2)
//      .flatMap(ob -> ob.topSellPrices(1).orElse(List.empty()))
//      .map(Quantity::getValue)
//      .toJavaStream()
//      .mapToDouble(BigDecimal::doubleValue)
//      .average()
//      .stream()
//      .flatMap(avg -> head._2.topSellPrice()
//        .map(sellPrice -> sellPrice.mapValue(value -> value.divide(BigDecimal.valueOf(avg), 4, RoundingMode.HALF_UP)))
//        .map(Quantity::getValue)
//        .stream()
//        .mapToDouble(BigDecimal::doubleValue))
//      .map(priceDiff -> 1d - priceDiff)
//      .findFirst();
//  }
//
//  public OptionalDouble averageBuyPricePercentChange() {
//    var stats = queryLastStats();
//    if (stats.isEmpty()) return OptionalDouble.empty();
//    var head = stats.head();
//    return stats
//      .map(t -> t._2)
//      .flatMap(ob -> ob.topBuyPrices(1).orElse(List.empty()))
//      .map(Quantity::getValue)
//      .toJavaStream()
//      .mapToDouble(BigDecimal::doubleValue)
//      .average()
//      .stream()
//      .flatMap(avg -> head._2.topBuyPrice()
//        .map(buyPrices -> buyPrices.mapValue(value -> value.divide(BigDecimal.valueOf(avg), 4, RoundingMode.HALF_UP)))
//        .map(Quantity::getValue)
//        .stream()
//        .mapToDouble(BigDecimal::doubleValue))
//      .map(priceDiff -> 1d - priceDiff)
//      .findFirst();
//  }


  @RequiredArgsConstructor
  public static class TotalStats {
    private final ZonedDateTime startTime;
    private final ZonedDateTime lastTime;
    final Quantity startMiddlePrice;
    final Quantity startBuyPrice;
    final Quantity startSellPrice;
    final double startSellToBuyRatio;

  }


  @RequiredArgsConstructor
  @Getter
  public static class OrderBookStats {
    private final ZonedDateTime time;
//    private final Quantity limitUp;
//    private final Quantity limitDown;
    private final List<Order> sellOrders;
    private final List<Order> buyOrders;

    public List<Quantity> sellPrices() {
      return sellOrders.map(Order::getPrice).map(Quantity::ofQuotation);
    }

    public List<Quantity> buyPrices() {
      return buyOrders.map(Order::getPrice).map(Quantity::ofQuotation);
    }

    public Optional<Quantity> topBuyPrice() {
      return buyOrders.headOption().map(Order::getPrice).map(Quantity::ofQuotation).toJavaOptional();
    }

    public Optional<Quantity> buyPriceDiff() {
      return buyOrders.lastOption()
        .map(Order::getPrice)
        .map(Quantity::ofQuotation)
        .toJavaOptional()
        .flatMap(lowestBuy -> topBuyPrice()
          .map(topBuy -> topBuy.subtract(lowestBuy)));
    }

    public Optional<Quantity> sellPriceDiff() {
      return sellOrders.lastOption()
        .map(Order::getPrice)
        .map(Quantity::ofQuotation)
        .toJavaOptional()
        .flatMap(lowestSell -> topSellPrice()
          .map(topSell -> topSell.subtract(lowestSell)));
    }

    public Optional<Quantity> calculatedMiddlePrice() {
      return topBuyPrice()
        .flatMap(topBuyPriceQ -> topSellPrice()
          .map(topSellPriceQ -> topBuyPriceQ
            .mapValue(topBuyPriceVal -> topBuyPriceVal
              .add(topSellPriceQ.getValue())
              .divide(BigDecimal.valueOf(2), 4, RoundingMode.HALF_UP)
            )));
    }

    public Optional<Quantity> topSellPrice() {
      return sellOrders.headOption().map(Order::getPrice).map(Quantity::ofQuotation).toJavaOptional();
    }


    public Quantity totalSellVolume() {
      return sellOrders.map(Order::getQuantity)
        .map(Quantity::ofUnits)
        .fold(Quantity.ZERO, Quantity::add);
    }

    public double sellToBuyRatio() {
      var buyVolQ = totalBuyVolume();
      if (buyVolQ.isEquals(Quantity.ZERO)) return 0d;
      return totalSellVolume()
        .mapValue(sellVol -> sellVol.divide(buyVolQ.getValue(), 4, RoundingMode.HALF_UP))
        .getValue()
        .doubleValue()
        ;
    }

    public Quantity totalBuyVolume() {
      return buyOrders.map(Order::getQuantity)
        .map(Quantity::ofUnits)
        .fold(Quantity.ZERO, Quantity::add);
    }


  }

}
