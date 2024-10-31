package ru.tinkoff.piapi.example;

import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Queue;
import io.vavr.collection.Set;
import io.vavr.control.Try;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.BaseStrategy;
import org.ta4j.core.BaseTradingRecord;
import org.ta4j.core.Rule;
import org.ta4j.core.Strategy;
import org.ta4j.core.TradingRecord;
import org.ta4j.core.indicators.EMAIndicator;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsLowerIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsMiddleIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.indicators.helpers.HighPriceIndicator;
import org.ta4j.core.indicators.helpers.LowPriceIndicator;
import org.ta4j.core.indicators.statistics.StandardDeviationIndicator;
import org.ta4j.core.num.DecimalNum;
import org.ta4j.core.rules.CrossedDownIndicatorRule;
import org.ta4j.core.rules.CrossedUpIndicatorRule;
import org.ta4j.core.rules.OverIndicatorRule;
import org.ta4j.core.rules.StopGainRule;
import org.ta4j.core.rules.StopLossRule;
import org.ta4j.core.rules.UnderIndicatorRule;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.OrderBook;
import ru.tinkoff.piapi.contract.v1.OrderDirection;
import ru.tinkoff.piapi.contract.v1.PostOrderAsyncResponse;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradingStatus;
import ru.tinkoff.piapi.core.ApiConfig;
import ru.tinkoff.piapi.core.InvestApi;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.core.utils.DateUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ru.tinkoff.piapi.core.utils.MapperUtils.quotationToBigDecimal;
import static ru.tinkoff.piapi.example.ExampleUtils.printPortfolioBalance;
import static ru.tinkoff.piapi.example.ExampleUtils.showOHLCPlot;
import static ru.tinkoff.piapi.example.ExampleUtils.writeMessagesToJsonFile;

@RequiredArgsConstructor
public class Example {

  private static final Logger log = LoggerFactory.getLogger(Example.class);

  private final List<MarketdataStreams.StreamView> streamViews;
  private final Map<String, TradingState> tradeStates;
  private final Map<String, Orders.IOrdersBox> ordersBox;
  private boolean isFinished = false;


  public static void main(String[] args) throws Exception {

    var api = InvestApi.create(ApiConfig.loadFromClassPath("example-bot.properties"));
    var instruments = new Instruments(api.getInstrumentsService());
    var marketdata = new Marketdata(api.getMarketDataService());
    var marketStream = new MarketdataStreams(api.getMarketDataStreamService());
    var operations = new Operations(api.getOperationsService(), api.getUserService());
    var tradingInstrumentsAll = instruments.tradeAvailableInstruments();
    instruments.saveTradingInstrument(tradingInstrumentsAll, "");
    var users = new Users(api.getUserService());
    var orders = new Orders(api.getOrdersService(), api.getOrdersStreamService());
    var userInfo = users.getUserInfo();
    var portfolio = operations.loadPortfolio(userInfo.getAccounts());

    printPortfolioBalance(portfolio);

    var topShares = tradingInstrumentsAll.select(Instruments.InstrumentObjType.SHARE, 10);

    var mdAllPrices = marketdata.getPricesAndCandles(new HashSet<>(topShares));

//    writeMessagesToJsonFile(mdAllPrices, ".", "closePrices.json", Marketdata.InstrumentWithPrice::getClosePrice)
//      .andFinally(() -> log.info("closePrices Saved"));

    showOHLCPlot(mdAllPrices);

    String accountId = portfolio.getPortfolios()
      .filter(es -> es._2._1.getTotalAmountCurrencies().getValue().compareTo(BigDecimal.ZERO) > 0)
      .maxBy(Comparator.comparingLong(es -> es._2._1.getTotalAmountCurrencies().getValue().longValue()))
      .map(Tuple2::_1)
      .getOrElseThrow(() -> new RuntimeException("No account with money"));

    log.info("selected account id:{}", accountId);

    var streamViews = marketStream.streamInstrument(
      "top10Shares",
      topShares.stream()
        .map(instrument -> Tuple.of(instrument, MD_LISTENER))
        .collect(Collectors.toSet())
    );

    var orderBoxes = topShares.stream().map(instrument -> orders.orderBox(instrument, accountId))
      .collect(Collectors.toMap(i -> i.instrument().getUuid(), v -> v));


    var bot = new Example(streamViews, initTradingStates(mdAllPrices), orderBoxes);
    boolean running = true;
    while (running) {
      bot.update();
      bot.think();
      bot.action();
      if (bot.isFinished) {
        running = false;
      }
      Thread.sleep(10_000);
    }

    System.exit(-1);


  }

  private void action() {
    tradeStates
      .entrySet()
      .stream()
      .filter(v -> !v.getValue().tradingSignals.isEmpty())
      .forEach(tradingStateEs -> {
        var tradingState = tradingStateEs.getValue();
        var signals = tradingState.tradingSignals;
        var iorderBox = ordersBox.get(tradingStateEs.getKey());
        signals
          .findLast(st -> st.signalType == TradingSignalType.ENTER)
          .peek(signal -> {
            var cmd = new Orders.LimitOrderCommand(
              OrderDirection.ORDER_DIRECTION_BUY,
              Quantity.ONE,
              new Quantity(signal.closePrice)
            );
            iorderBox.execute(cmd);
            var deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
            while (System.currentTimeMillis() < deadline) {
              var status = iorderBox.status(cmd);
              if (status.getResultCall().isLeft()) {
                this.isFinished = true;
              }
              if (status.getResultCall().isRight() && status.getResultCall().get() instanceof CompletableFuture) {
                var sleepDone = Try.run(() -> Thread.sleep(1_000)).isSuccess();
                this.isFinished = sleepDone;
                if (!sleepDone) break;
              } else if (status.getResultCall().isRight() && status.getResultCall().get() instanceof PostOrderAsyncResponse) {
                var postOrderAsyncResponse = (PostOrderAsyncResponse) status.getResultCall().get();
                log.info("order complete", postOrderAsyncResponse);
              }
            }
          });

        signals.findLast(st -> st.signalType == TradingSignalType.EXIT)
          .peek(signal -> {
            iorderBox.execute(new Orders.LimitOrderCommand(
              OrderDirection.ORDER_DIRECTION_SELL,
              Quantity.ONE,
              new Quantity(signal.closePrice)
            ));
          });


      });

  }

  private void think() {

    List<Tuple2<String, TradingSignal>> signals = new ArrayList<>();
    tradeStates.forEach((instrumentUid, state) -> {
      var signal = state.runStrategy();
      signals.add(Tuple.of(instrumentUid, signal));
    });

    signals.forEach(tuple -> tuple.apply((instrumentUid, signal) -> {
      tradeStates.computeIfPresent(instrumentUid, (k, v) -> v.applySignal(signal));
      return null;
    }));

  }

  private void update() {
    streamViews.forEach(streamView -> {
      streamView.instruments()
        .map(instrument -> streamView.get(instrument))
        .forEach(mdCtx -> {
          if (mdCtx.isPresent()) {
            var md = mdCtx.get();
            var instrument = md.getInstrument();
            tradeStates.compute(instrument.getUuid(), (k, v) -> {
              if (v == null) v = new TradingState(buildBarSeries(instrument.toString(), 500)
                , new BaseTradingRecord());
              return v.updateSeries(
                barSeries -> md.getCandles().forEach(candle -> addCandle(barSeries, candle))
              );
            });
          }
        });

    });
  }

  private static Map<String, TradingState> initTradingStates(List<Marketdata.InstrumentWithPrice> mdAllPrices) {
    return mdAllPrices.stream()
      .map(instrumentWithPrice -> {
        var barSeries = buildBarFromHistory(instrumentWithPrice.getInstrument(), instrumentWithPrice.getCandles());
        return Tuple.of(instrumentWithPrice.getInstrument().getUuid(), new TradingState(barSeries, new BaseTradingRecord()));
      })
      .collect(Collectors.toMap(v -> v._1, v -> v._2));
  }


  @RequiredArgsConstructor
  static class TradingState {
    private final BarSeries barSeries;
    private final TradingRecord tradingRecord;
    private final Strategy strategy;
    private final io.vavr.collection.Set<TradingSignal> tradingSignals;

    public TradingState(BarSeries barSeries, TradingRecord tradingRecord) {
      this(
        barSeries,
        tradingRecord,
        buildStrategy(barSeries, tradingRecord),
        io.vavr.collection.HashSet.empty()
      );
    }


    TradingState updateSeries(Consumer<BarSeries> modifier) {
      modifier.accept(barSeries);
      return this;
    }

    TradingState applySignal(TradingSignal signals) {
      return new TradingState(barSeries, tradingRecord, strategy, tradingSignals.add(signals));
    }

    TradingSignal runStrategy() {
      if (barSeries.isEmpty()) return new TradingSignal(TradingSignalType.WAIT, null);
      int endIndex = barSeries.getEndIndex();
      var newBar = barSeries.getBar(endIndex);
      var closePrice = (BigDecimal) newBar.getClosePrice().getDelegate();
      if (strategy.shouldEnter(endIndex, tradingRecord)) {
        log.info("Strategy should ENTER on {}", endIndex);
        return new TradingSignal(TradingSignalType.ENTER, closePrice);
      } else if (strategy.shouldExit(endIndex, tradingRecord)) {
        return new TradingSignal(TradingSignalType.EXIT, closePrice);
      }
      return new TradingSignal(TradingSignalType.WAIT, closePrice);
    }
  }

  @RequiredArgsConstructor
  @Getter
  static class TradingSignal {
    private final TradingSignalType signalType;
    private final BigDecimal closePrice;
  }

  enum TradingSignalType {
    WAIT, ENTER, EXIT, ENTER_DONE, EXIT_DONE,
    ENTER_IN_PROGRESS,
    EXIT_IN_PROGRESS
  }

  class InstrumentTradingState {
    private final String id = ";";
    private final AtomicReference<InstrumentTradingStateEnum> state = new AtomicReference<>(InstrumentTradingStateEnum.INIT);
    private final AtomicReference<Queue<MarketDataResponse>> onlineCandles = new AtomicReference<>(Queue.empty());
    private final AtomicReference<Quantity> lastPrice = new AtomicReference<>(null);
    private final Set<Strategy> strategies;
    private final List<HistoricCandle> historicDayCandles = List.of();
    private final List<HistoricCandle> historicMinutesCandles = List.of();

    private BarSeries minuteBarSeries;

    InstrumentTradingState(Set<Strategy> strategies) {
      this.strategies = strategies;
    }

//    PriorityQueue<Strategy> toPriorityQueue() {
//      new ReturnCriterion().calculate(series, tradingRecord)
//      return strategies.toPriorityQueue(Comparator.comparingDouble(strategy->strategy));
//    }

  }

  enum InstrumentTradingStateEnum {
    INIT, INIT_COMPLETE,
    SYNC, SYNC_COMPLETE,
  }

  static BarSeries buildBarFromHistory(Instruments.Instrument instrument, List<HistoricCandle> candles) {
    var barDuration = Duration.ofMinutes(1);
    var barSeries = buildBarSeries(instrument.toString(), 500, candles, (series, candle) -> {
      var barEndTime = ZonedDateTime.ofInstant(DateUtils.timestampToInstant(candle.getTime()), ZoneId.systemDefault());
      return new BaseBar(
        barDuration,
        barEndTime,
        quotationToBigDecimal(candle.getOpen()),
        quotationToBigDecimal(candle.getHigh()),
        quotationToBigDecimal(candle.getLow()),
        quotationToBigDecimal(candle.getClose()),
        BigDecimal.valueOf(candle.getVolume())
      );
    });
    return barSeries;
  }

  static BarSeries addCandle(BarSeries series, Candle candle) {
    var lastCandleEndOpt = Optional.of(series)
      .filter(s -> s.getBarCount() > 0)
      .map(BarSeries::getLastBar)
      .map(Bar::getEndTime);
    var candleTime = ZonedDateTime.ofInstant(DateUtils.timestampToInstant(candle.getTime()), ZoneId.systemDefault());
    return lastCandleEndOpt.map(lastCandleEnd -> {
        Try.run(() -> {
            if (candleTime.isEqual(lastCandleEnd)) {
              series.addBar(
                new BaseBar(
                  Duration.ofMinutes(1),
                  candleTime,
                  DecimalNum.valueOf(quotationToBigDecimal(candle.getOpen())),
                  DecimalNum.valueOf(quotationToBigDecimal(candle.getHigh())),
                  DecimalNum.valueOf(quotationToBigDecimal(candle.getLow())),
                  DecimalNum.valueOf(quotationToBigDecimal(candle.getClose())),
                  DecimalNum.valueOf(candle.getVolume()),
                  DecimalNum.valueOf(0)
                ),
                true
              );
            } else {
              series.addBar(
                Duration.ofMinutes(1),
                candleTime,
                DecimalNum.valueOf(quotationToBigDecimal(candle.getOpen())),
                DecimalNum.valueOf(quotationToBigDecimal(candle.getHigh())),
                DecimalNum.valueOf(quotationToBigDecimal(candle.getLow())),
                DecimalNum.valueOf(quotationToBigDecimal(candle.getClose())),
                DecimalNum.valueOf(candle.getVolume()),
                DecimalNum.valueOf(0)
              );
            }
          })
          .onFailure(error -> log.error("cannot apply state update", error))
          .andFinally(() -> log.debug("last state index:{}", series.getEndIndex()))
        ;
        return series;
      })
      .orElse(series);
  }

  private static Strategy buildStrategy(BarSeries series, TradingRecord tradingRecord) {
    var minBarCount = 10;
    var takeProfitValue = DecimalNum.valueOf(2);
    ClosePriceIndicator closePrice = new ClosePriceIndicator(series);
    LowPriceIndicator lowPriceIndicator = new LowPriceIndicator(series);

    HighPriceIndicator highPriceIndicator = new HighPriceIndicator(series);
    SMAIndicator sma = new SMAIndicator(closePrice, minBarCount);
    EMAIndicator ema = new EMAIndicator(closePrice, minBarCount);
    BollingerBandsMiddleIndicator middleIndicator = new BollingerBandsMiddleIndicator(ema);
    StandardDeviationIndicator standardDeviationIndicator = new StandardDeviationIndicator(closePrice, minBarCount);
    BollingerBandsLowerIndicator lowerIndicator = new BollingerBandsLowerIndicator(middleIndicator, standardDeviationIndicator);

    Rule entrySignal1 = new OverIndicatorRule(closePrice, sma);
    Rule entrySignal2 = new UnderIndicatorRule(lowPriceIndicator, lowerIndicator);
    Rule exitSignal = new CrossedDownIndicatorRule(closePrice, lowerIndicator);
    Rule exitSignal2 = new StopGainRule(closePrice, takeProfitValue);

    Rule sentrySignal = new CrossedDownIndicatorRule(lowPriceIndicator, lowerIndicator);
    Rule sentrySignal2 = new OverIndicatorRule(highPriceIndicator, lowPriceIndicator);

    Rule sexitSignal = new CrossedUpIndicatorRule(closePrice, lowerIndicator);
    Rule sexitSignal2 = new StopLossRule(closePrice, takeProfitValue);

    return new BaseStrategy(entrySignal1.and(entrySignal2), exitSignal.or(exitSignal2))
      .or(new BaseStrategy(sentrySignal.and(sentrySignal2), sexitSignal.or(sexitSignal2)))
      ;
  }

  static <T> BarSeries buildBarSeries(String nm, int maxBarCount) {
    BarSeries series = new BaseBarSeries(nm);
    series.setMaximumBarCount(maxBarCount);
    return series;
  }

  static <T> BarSeries buildBarSeries(String nm, int maxBarCount, List<T> bars, Function2<BarSeries, T, Bar> func) {
    BarSeries series = new BaseBarSeries(nm);
    series.setMaximumBarCount(maxBarCount);
    bars.forEach(barModel -> series.addBar(func.apply(series, barModel)));
    return series;
  }

  static MarketdataStreams.InstrumentMdListener MD_LISTENER = new MarketdataStreams.InstrumentMdListener() {
    @Override
    public void onCandleTick(Candle candle, MarketdataStreams.InstrumentMdContext ctx) {
      log.info("Candle tick: {}", candle);
    }

    @Override
    public void onTradeTick(Trade trade, MarketdataStreams.InstrumentMdContext ctx) {
      log.info("Trade tick: {}", trade);
    }

    @Override
    public void onLastPriceTick(LastPrice lastPrice, MarketdataStreams.InstrumentMdContext ctx) {
      log.info("Last price tick: {}", lastPrice);
    }

    @Override
    public void onOrderBookTick(OrderBook orderbook, MarketdataStreams.InstrumentMdContext ctx) {
      log.info("Orderbook tick: {}", orderbook);
    }

    @Override
    public void onTradingStatusTick(TradingStatus tradingStatus, MarketdataStreams.InstrumentMdContext ctx) {
      log.info("Trading status tick: {}", tradingStatus);
    }
  };
}
