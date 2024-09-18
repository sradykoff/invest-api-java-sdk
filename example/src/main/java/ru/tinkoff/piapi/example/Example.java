package ru.tinkoff.piapi.example;

import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.collection.PriorityQueue;
import io.vavr.collection.Queue;
import io.vavr.collection.Set;
import io.vavr.collection.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.Strategy;
import org.ta4j.core.criteria.pnl.ReturnCriterion;
import org.ta4j.core.num.DecimalNum;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.GetAssetFundamentalsResponse;
import ru.tinkoff.piapi.contract.v1.GetTradingStatusResponse;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.InstrumentStatus;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.SecurityTradingStatus;
import ru.tinkoff.piapi.contract.v1.Share;
import ru.tinkoff.piapi.contract.v1.TradingSchedule;
import ru.tinkoff.piapi.core.ApiConfig;
import ru.tinkoff.piapi.core.InvestApi;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.core.stream.MarketDataSubscriptionService;
import ru.tinkoff.piapi.core.stream.StreamProcessor;
import ru.tinkoff.piapi.core.utils.DateUtils;
import ru.tinkoff.piapi.core.utils.MapperUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Example {

  private static final int MAX_CANDLE_DAY_HISTORY = 30;
  private static final int MAX_CANDLE_MINUTES_HISTORY = 30;
  private static final Logger log = LoggerFactory.getLogger(Example.class);
  private final InvestApi api;
  private final ConcurrentMap<String, InstrumentTradingState> tradingStates;
  private final ScheduledExecutorService executor;

  public Example(InvestApi api) {
    this.api = api;
    this.tradingStates = new ConcurrentHashMap<>();
    this.executor = Executors.newScheduledThreadPool(4);
  }

  public static void main(String[] args) throws Exception {

    var api = InvestApi.create(ApiConfig.loadFromClassPath("example-bot.properties"));

    Instruments.saveTradingInstrument("",api);

    System.exit(-1);

    var bot = new Example(api);
    //select trading instruments


    var sharesFuture = bot.selectTradingShares(50);
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


    log.info("instrument:{}", sharesFuture.get());
  }

  CompletableFuture<List<Share>> selectTradingShares(int totalSize) {
    return api.runWithHeaders(
      (call, headers) -> call.getInstrumentsService()
        .getShares(InstrumentStatus.INSTRUMENT_STATUS_BASE)
        .whenCompleteAsync(
          (result, throwable) -> log.info("headers:{} trackingId:{}", headers, headers.get("x-tracking-id")),
          executor
        )
        .thenApply(shares -> shares.stream()
          .filter(Share::getLiquidityFlag)
          .filter(share -> !share.getForQualInvestorFlag())
          .filter(Share::getBuyAvailableFlag)
          .filter(Share::getSellAvailableFlag)
          .filter(Share::getShortEnabledFlag)
          .collect(Collectors.toList()))

        .thenApply(shares -> Tuple.of(
          shares,
          Stream.ofAll(shares).sliding(99)
            .map(sharesChunk -> call.getInstrumentsService()
              .getAssetFundamentals(
                sharesChunk.toJavaStream()
                  .map(Share::getAssetUid)
                  .collect(Collectors.toSet())
              ))))
        .thenCompose(shareTuple -> shareTuple.map2(
              assetsFutures -> assetsFutures
                .reduceLeft((left, right) -> left
                  .thenCompose(leftResult -> right
                    .thenApply(
                      rightResult -> leftResult.toBuilder()
                        .addAllFundamentals(rightResult.getFundamentalsList())
                        .build()
                    )
                  )
                )
            )
            ._2
            .thenApply(assetFundamentalsResponse -> {
              var shares = shareTuple._1;
              if (assetFundamentalsResponse.getFundamentalsCount() > 0) {
                var topAssets = assetFundamentalsResponse
                  .getFundamentalsList()
                  .stream()
                  .sorted(Comparator.comparingDouble(
                        GetAssetFundamentalsResponse.StatisticResponse::getAverageDailyVolumeLast4Weeks
                      )
                      .thenComparingDouble(GetAssetFundamentalsResponse.StatisticResponse::getRoe)
                      .thenComparingDouble(GetAssetFundamentalsResponse.StatisticResponse::getOneYearAnnualRevenueGrowthRate)

                  )
                  .limit(totalSize)
                  .peek(statisticResponse -> log.info("asset:{}", statisticResponse))
                  .map(GetAssetFundamentalsResponse.StatisticResponse::getAssetUid)
                  .collect(Collectors.toSet());

                return shares
                  .stream()
                  .filter(share -> topAssets.contains(share.getAssetUid()))
                  .collect(Collectors.toList());
              }
              return shares
                .stream()
                .limit(totalSize)
                .collect(Collectors.toList());
            })
        )
    );
  }

  CompletableFuture<SecurityTradingStatus> getTradingStatus(String instrumentUid) {
    return api.getMarketDataService().getTradingStatus(instrumentUid)
      .thenApply(GetTradingStatusResponse::getTradingStatus);
  }

  CompletableFuture<List<TradingSchedule>> getTradingSchedules() {
    return api.getInstrumentsService().getTradingSchedules(Instant.now().minus(7, ChronoUnit.DAYS), Instant.now());
  }

  void marketDataStream(
    String id,
    Consumer<MarketDataSubscriptionService> onSubscribe,
    StreamProcessor<MarketDataResponse> processor,
    Consumer<Throwable> onError
  ) {
    var mdStream = api.getMarketDataStreamService().newStream(
      id,
      processor,
      onError
    );
    onSubscribe.accept(mdStream);
  }

  void positionsStream(

  ) {

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
          MapperUtils.quotationToBigDecimal(candle.getOpen()),
          MapperUtils.quotationToBigDecimal(candle.getHigh()),
          MapperUtils.quotationToBigDecimal(candle.getLow()),
          MapperUtils.quotationToBigDecimal(candle.getClose()),
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
}
