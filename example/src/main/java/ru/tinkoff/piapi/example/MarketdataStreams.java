package ru.tinkoff.piapi.example;

import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import lombok.RequiredArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.OrderBook;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradingStatus;
import ru.tinkoff.piapi.core.stream.MarketDataStreamService;
import ru.tinkoff.piapi.core.stream.MarketDataSubscriptionService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

@Slf4j
@RequiredArgsConstructor
public class MarketdataStreams {
  private final MarketDataStreamService streamService;
  private final AtomicInteger counter = new AtomicInteger(1);

  public List<StreamView> streamInstrument(String nm, Set<Tuple2<Instruments.Instrument, InstrumentMdListener>> instrumentsHandler) {

    return Stream.ofAll(instrumentsHandler)
      .sliding(100)
      .map(batch -> {

        ConcurrentMap<String, AtomicReference<DataHolder>> concurrentMap = new ConcurrentHashMap<>();

        batch.forEach(instrumentHandler -> concurrentMap
          .put(
            instrumentHandler._1.getUuid(),
            new AtomicReference<>(EMPTY.withListener(instrumentHandler._2))
          ));

        var instruments = batch.map(t -> t.apply((instrument, consumer) -> instrument));
        var ids = instruments
          .map(Instruments.Instrument::getUuid)
          .toJavaList();
        var service = streamService.newStream(
          nm + counter.getAndIncrement(),
          response -> applyData(concurrentMap, response),
          error -> log.error("stream error", error)
        );


        service.subscribeCandles(ids, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE, true);
        service.subscribeLastPrices(ids);
        service.subscribeTrades(ids);
        service.subscribeOrderbook(ids, 10);
        service.subscribeInfo(ids);


        return (StreamView) new StreamViewImpl(instruments, concurrentMap, service);

      })
      .toJavaList();


  }

  private void applyData(ConcurrentMap<String, AtomicReference<DataHolder>> concurrentMap, MarketDataResponse response) {
    var responseCase = response.getPayloadCase();
    String instrumentId = null;
    Consumer<DataHolder> notifier = __ -> {
    };
    UnaryOperator<DataHolder> updateFunc = UnaryOperator.identity();
    switch (responseCase) {
      case CANDLE:
        var candle = response.getCandle();
        updateFunc = dataHolder -> dataHolder.onCandle(candle);
        notifier = dataHolder -> dataHolder.publish(candle);
        instrumentId = candle.getInstrumentUid();
        break;
      case TRADE:
        var trade = response.getTrade();
        updateFunc = dataHolder -> dataHolder.onTrade(trade);
        notifier = dataHolder -> dataHolder.publish(trade);
        instrumentId = trade.getInstrumentUid();
        break;
      case LAST_PRICE:
        var lastPrice = response.getLastPrice();
        updateFunc = dataHolder -> dataHolder.onLastPrice(lastPrice);
        notifier = dataHolder -> dataHolder.publish(lastPrice);
        instrumentId = lastPrice.getInstrumentUid();
        break;
      case ORDERBOOK:
        var orderbook = response.getOrderbook();
        updateFunc = dataHolder -> dataHolder.onOrderbook(orderbook);
        notifier = dataHolder -> dataHolder.publish(orderbook);
        instrumentId = orderbook.getInstrumentUid();
        break;
      case TRADING_STATUS:
        var tradingStatus = response.getTradingStatus();
        updateFunc = dataHolder -> dataHolder.onTradingStatus(tradingStatus);
        notifier = dataHolder -> dataHolder.publish(tradingStatus);
        instrumentId = tradingStatus.getInstrumentUid();
        break;
      case SUBSCRIBE_CANDLES_RESPONSE:
        var candleResponse = response.getSubscribeCandlesResponse();
        return;
      case SUBSCRIBE_INFO_RESPONSE:
        var infoResponse = response.getSubscribeInfoResponse();
        return;
      case SUBSCRIBE_LAST_PRICE_RESPONSE:
        var lastPriceResponse = response.getSubscribeLastPriceResponse();
        return;
      case SUBSCRIBE_ORDER_BOOK_RESPONSE:
        var orderBookResponse = response.getSubscribeOrderBookResponse();
        return;
      case SUBSCRIBE_TRADES_RESPONSE:
        var tradesResponse = response.getSubscribeTradesResponse();
        return;
      case PING:
        return;
    }
    var holder = concurrentMap.computeIfAbsent(instrumentId, k -> new AtomicReference<>(EMPTY));

    var nextHolder = holder.updateAndGet(updateFunc);

    notifier.accept(nextHolder);

  }

  @RequiredArgsConstructor
  static class StreamViewImpl implements StreamView {
    private final Stream<Instruments.Instrument> instruments;
    private final ConcurrentMap<String, AtomicReference<DataHolder>> concurrentMap;
    private final MarketDataSubscriptionService streamService;

    public Stream<Instruments.Instrument> instruments() {
      return instruments;
    }
  }

  private static final DataHolder EMPTY = new DataHolder(
    null,
    io.vavr.collection.List.empty(),
    io.vavr.collection.List.empty(),
    io.vavr.collection.List.empty(),
    io.vavr.collection.List.empty(),
    io.vavr.collection.List.empty(),
    0
  );

  @RequiredArgsConstructor
  @With
  static class DataHolder implements InstrumentMdContext {

    private final InstrumentMdListener listener;
    private final io.vavr.collection.List<Candle> candles;
    private final io.vavr.collection.List<Trade> trades;
    private final io.vavr.collection.List<LastPrice> lastPrices;
    private final io.vavr.collection.List<OrderBook> orderbooks;
    private final io.vavr.collection.List<TradingStatus> tradingStatuses;
    private final int version;


    public DataHolder onCandle(Candle candle) {
      var buffer = candles.append(candle);
      if (buffer.size() > 100) {
        buffer = buffer.dropRight(1);
      }
      return withCandles(buffer)
        .withVersion(version + 1);
    }

    public DataHolder onTrade(Trade trade) {
      var buffer = trades.append(trade);
      if (buffer.size() > 100) {
        buffer = buffer.dropRight(1);
      }
      return withTrades(buffer).withVersion(version + 1);
    }

    public DataHolder onLastPrice(LastPrice lastPrice) {
      var buffer = lastPrices.append(lastPrice);
      if (buffer.size() > 100) {
        buffer = buffer.dropRight(1);
      }
      return withLastPrices(buffer).withVersion(version + 1);
    }

    public DataHolder onOrderbook(OrderBook orderbook) {
      var buffer = orderbooks.append(orderbook);
      if (buffer.size() > 100) {
        buffer = buffer.dropRight(1);
      }
      return withOrderbooks(buffer).withVersion(version + 1);
    }

    public DataHolder onTradingStatus(TradingStatus tradingStatus) {
      var buffer = tradingStatuses.append(tradingStatus);
      if (buffer.size() > 100) {
        buffer = buffer.dropRight(1);
      }
      return withTradingStatuses(buffer).withVersion(version + 1);
    }

    public void publish(Candle candle) {
      if (listener != null) {
        listener.onCandleTick(candle, this);
      }
    }

    public void publish(Trade trade) {
      if (listener != null) {
        listener.onTradeTick(trade, this);
      }
    }

    public void publish(LastPrice lastPrice) {
      if (listener != null) {
        listener.onLastPriceTick(lastPrice, this);
      }
    }

    public void publish(OrderBook orderbook) {
      if (listener != null) {
        listener.onOrderBookTick(orderbook, this);
      }
    }

    public void publish(TradingStatus tradingStatus) {
      if (listener != null) {
        listener.onTradingStatusTick(tradingStatus, this);
      }
    }
  }

  public interface InstrumentMdContext {

  }

  public interface InstrumentMdListener {
    default void onCandleTick(Candle candle, InstrumentMdContext ctx) {
    }

    default void onTradeTick(Trade trade, InstrumentMdContext ctx) {
    }

    default void onLastPriceTick(LastPrice lastPrice, InstrumentMdContext ctx) {
    }

    default void onOrderBookTick(OrderBook orderbook, InstrumentMdContext ctx) {
    }

    default void onTradingStatusTick(TradingStatus tradingStatus, InstrumentMdContext ctx) {
    }
  }

  public interface StreamView {

  }
}
