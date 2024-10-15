package ru.tinkoff.piapi.example;

import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.collection.Queue;
import io.vavr.collection.Set;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.Strategy;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.OrderBook;
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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static ru.tinkoff.piapi.core.utils.MapperUtils.quotationToBigDecimal;
import static ru.tinkoff.piapi.example.ExampleUtils.printPortfolioBalance;
import static ru.tinkoff.piapi.example.ExampleUtils.showOHLCPlot;
import static ru.tinkoff.piapi.example.ExampleUtils.writeMessagesToJsonFile;

@RequiredArgsConstructor
public class Example {

  private static final Logger log = LoggerFactory.getLogger(Example.class);

  private final MarketdataStreams.StreamView streamView;
  private final BarSeries series;
  private final Strategy strategy;
  private final Orders.OrdersBox ordersBox;


  public static void main(String[] args) throws Exception {

    var api = InvestApi.create(ApiConfig.loadFromClassPath("example-bot.properties"));
    var instruments = new Instruments(api.getInstrumentsService());
    var marketdata = new Marketdata(api.getMarketDataService());
    var marketStream = new MarketdataStreams(api.getMarketDataStreamService());
    var operations = new Operations(api.getOperationsService(), api.getUserService());
    var tradingInstrumentsAll = instruments.tradeAvailableInstruments();
    instruments.saveTradingInstrument(tradingInstrumentsAll, "");
    var users = new Users(api.getUserService());
    var userInfo = users.getUserInfo();
    var portfolio = operations.loadPortfolio(userInfo.getAccounts());

    printPortfolioBalance(portfolio);

    var topShares = tradingInstrumentsAll.select(Instruments.InstrumentObjType.SHARE, 10);

    var mdAllPrices = marketdata.getPricesAndCandles(new HashSet<>(topShares));

//    writeMessagesToJsonFile(mdAllPrices, ".", "closePrices.json", Marketdata.InstrumentWithPrice::getClosePrice)
//      .andFinally(() -> log.info("closePrices Saved"));

    showOHLCPlot(mdAllPrices);


    var streamViews = marketStream.streamInstrument(
      "top10Shares",
      topShares.stream()
        .map(instrument -> Tuple.of(instrument, MD_LISTENER))
        .collect(Collectors.toSet())
    );



    Thread.sleep(120_000L);


    System.exit(-1);

    var bot = new Example(api);
    //select trading instruments


    //setup statistic and watch

    //sharesFuture.thenCompose(shares -> api.getMarketDataService().getTradingStatus(share))

//    sharesFuture.thenCompose(
//      shares -> shares.stream()
//        .map(share -> Tuple.of(
//          share,
//          api.getMarketDataService().getCandles(share.getUid()
//            , Instant.now().minus(MAX_CANDLE_DAY_HISTORY, ChronoUnit.DAYS)
//            , Instant.now()
//            , CandleInterval.CANDLE_INTERVAL_DAY
//          ),
//          api.getMarketDataService().getCandles(
//            share.getUid(),
//            Instant.now().minus(MAX_CANDLE_DAY_HISTORY, ChronoUnit.MINUTES),
//            Instant.now(),
//            CandleInterval.CANDLE_INTERVAL_1_MIN
//          )))
//
//    )


    //start trading

    //check trading status

    //stop trading

    //check operations


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

    InstrumentTradingState initHistoryMinuteCandle(List<HistoricCandle> candles) {
      var barDuration = Duration.ofSeconds(60);
      var barSeries = buildBarSeries(id + "-minute", 1000, candles, (series, candle) -> {
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
      this.minuteBarSeries = barSeries;
      return this;
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
