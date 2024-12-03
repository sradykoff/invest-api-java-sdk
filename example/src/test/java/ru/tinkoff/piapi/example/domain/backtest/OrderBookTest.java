package ru.tinkoff.piapi.example.domain.backtest;

import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.trading.OrderId;

import static org.junit.jupiter.api.Assertions.*;

class OrderBookTest {

  final OrderBook orderBook = new OrderBook(new InstrumentId("ANY-ID"));

  @org.junit.jupiter.api.Test
  void runExecution() {
    var nextOrderBook = orderBook.addOrder(
      new OrderId("ORDER-id"),
      Quantity.ONE,
      Quantity.ONE,
      OrderBook.OrderSide.BID
    );

    nextOrderBook =  nextOrderBook.addOrder(
      new OrderId("ORDER-id"),
      Quantity.ofUnits(2),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.ASK
    );

    nextOrderBook = nextOrderBook.addOrder(
      new OrderId("ORDER-id"),
      Quantity.ONE,
      Quantity.ONE,
      OrderBook.OrderSide.ASK
    );

    nextOrderBook = nextOrderBook.addOrder(
      new OrderId("ORDER-id"),
      Quantity.ofUnits(2),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.ASK
    );

    var bidOrdersMap = nextOrderBook.getOrdersMap(OrderBook.OrderSide.BID);
    var askOrdersMap = nextOrderBook.getOrdersMap(OrderBook.OrderSide.ASK);


    assertEquals(1,  bidOrdersMap.size());
    assertEquals(2,  askOrdersMap.size());

    System.out.println("BID: " + bidOrdersMap.toString());
    System.out.println("ASK: " + askOrdersMap.toString());

  }
}
