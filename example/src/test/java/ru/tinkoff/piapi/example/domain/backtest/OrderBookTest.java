package ru.tinkoff.piapi.example.domain.backtest;

import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.trading.OrderId;

import static org.junit.jupiter.api.Assertions.*;

class OrderBookTest {

  final OrderBook orderBook = new OrderBook(new InstrumentId("ANY-ID"));

  @org.junit.jupiter.api.Test
  void runExecution() {
    var nextOrderBookTuple = orderBook.addOrder(
      new OrderId("ORDER-id-1"),
      Quantity.ofUnits(100),
      Quantity.ONE,
      OrderBook.OrderSide.BID
    );

    nextOrderBookTuple =  nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-2"),
      Quantity.ofUnits(2),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.ASK
    );

    nextOrderBookTuple = nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-3"),
      Quantity.ONE,
      Quantity.ONE,
      OrderBook.OrderSide.ASK
    );

    nextOrderBookTuple = nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-4"),
      Quantity.ofUnits(2),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.ASK
    );

    var bidOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.BID);
    var askOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.ASK);

    System.out.println("BID: " + bidOrdersMap.toString());
    System.out.println("ASK: " + askOrdersMap.toString());

    assertEquals(1,  bidOrdersMap.size());
    assertEquals(1,  askOrdersMap.size());



    nextOrderBookTuple = nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-5"),
      Quantity.ofUnits(5),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.BID
    );

    bidOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.BID);
    askOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.ASK);

    System.out.println("BID: " + bidOrdersMap.toString());
    System.out.println("ASK: " + askOrdersMap.toString());

    assertEquals(2,  bidOrdersMap.size());
    assertEquals(0,  askOrdersMap.size());


    nextOrderBookTuple = nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-6"),
      Quantity.ofUnits(1),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.BID
    );

    bidOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.BID);
    askOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.ASK);

    System.out.println("BID: " + bidOrdersMap.toString());
    System.out.println("ASK: " + askOrdersMap.toString());

    assertEquals(2,  bidOrdersMap.size());
    assertEquals(0,  askOrdersMap.size());

    nextOrderBookTuple = nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-7"),
      Quantity.ofUnits(1),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.BID
    );

    bidOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.BID);
    askOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.ASK);

    System.out.println("BID: " + bidOrdersMap.toString());
    System.out.println("ASK: " + askOrdersMap.toString());

    assertEquals(2,  bidOrdersMap.size());
    assertEquals(0,  askOrdersMap.size());


    nextOrderBookTuple = nextOrderBookTuple._1.addOrder(
      new OrderId("ORDER-id-8"),
      Quantity.ofUnits(1),
      Quantity.ofUnits(2),
      OrderBook.OrderSide.ASK
    );

    bidOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.BID);
    askOrdersMap = nextOrderBookTuple._1.getOrdersMap(OrderBook.OrderSide.ASK);

    System.out.println("BID: " + bidOrdersMap.toString());
    System.out.println("ASK: " + askOrdersMap.toString());

    assertEquals(2,  bidOrdersMap.size());
    assertEquals(0,  askOrdersMap.size());




  }
}
