package ru.tinkoff.piapi.example.trading;

import lombok.extern.slf4j.Slf4j;
import org.ta4j.core.BaseTradingRecord;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.core.ApiConfig;
import ru.tinkoff.piapi.core.InvestApi;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.core.stream.StreamProcessor;
import ru.tinkoff.piapi.example.bars.CandleBarSeries;
import ru.tinkoff.piapi.example.domain.AccountId;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.strategies.MovingMomentumStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class LiveCandleTradeRunner {
  private static final int MAX_BAR_SIZE = 1200;
  private final InvestApi api;
  private final LiveTradeOrderManager orderManager;
  private final ConcurrentMap<InstrumentId, LiveCandleTradingBot> bots = new ConcurrentHashMap<>();


  public LiveCandleTradeRunner(Set<InstrumentId> instrumentIds, InvestApi api, AccountId accountId) {
    this.api = api;
    this.orderManager = new LiveTradeOrderManager(api.getOrdersService(), accountId);
    instrumentIds.forEach(instrumentId -> {

      var cbSeries = new CandleBarSeries(
        instrumentId.toString(),
        Duration.ofMinutes(1),
        MAX_BAR_SIZE
      );

      bots.put(
        instrumentId,
        new LiveCandleTradingBot(
          instrumentId,
          cbSeries,
          MovingMomentumStrategy.buildStrategy(cbSeries.getSeries()),
          new BaseTradingRecord()
        )
      );
    });
  }

  void loadHistoryCandles() {
    bots.values().forEach(bot -> {
      var historicCandles = api.getMarketDataService()
        .getCandlesSync(
          bot.getInstrumentId().getId(),
          Instant.now().minusSeconds(24 * 3600),
          Instant.now(),
          CandleInterval.CANDLE_INTERVAL_1_MIN
        );
      bot.getCandleBarSeries().addFromHistory(historicCandles);
    });
  }

  public void onCandle(Candle candle) {
    var instrumentId = new InstrumentId(candle.getInstrumentUid());
    var liveBot = bots.get(instrumentId);
    if (liveBot == null) {
      log.error("Bot not found for instrument: {}", instrumentId);
      return;
    }
    var nextSignal = liveBot.signalOnCandle(candle);

    if (nextSignal.isBuy()) {
      log.debug("signal buy: {}", nextSignal);
      orderManager.buyLimit(nextSignal.getInstrumentId(), Quantity.ONE, nextSignal.closePrice);
    } else if (nextSignal.isSell()) {
      log.debug("signal sell: {}", nextSignal);
      orderManager.sellLimit(nextSignal.getInstrumentId(), Quantity.ONE, nextSignal.closePrice);
    } else {
      log.debug("signal hold: {}", nextSignal);
    }
  }


  public static void main(String[] args) {
    var api = InvestApi.create(ApiConfig
      .loadFromClassPath("example-bot.properties"));

    var stopSignalCf = new CompletableFuture<>();

    var instrumentSet = Set.of(
      new InstrumentId("e6123145-9665-43e0-8413-cd61b8aa9b13"),
      new InstrumentId("46ae47ee-f409-4776-bf20-43a040b9e7fb"),
      new InstrumentId("0a55e045-e9a6-42d2-ac55-29674634af2f"),
      new InstrumentId("77cb416f-a91e-48bd-8083-db0396c61a41")

    );

    var tradeBotRunner = new LiveCandleTradeRunner(instrumentSet, api, new AccountId("2147842561"));

    tradeBotRunner.loadHistoryCandles();

    var stream = api.getMarketDataStreamService().newStream("MD-stream", new StreamProcessor<MarketDataResponse>() {
      @Override
      public void process(MarketDataResponse response) {
        if (response.hasCandle()) tradeBotRunner.onCandle(response.getCandle());
      }
    }, error -> {
      log.error("stream error", error);
      stopSignalCf.completeExceptionally(error);
    });


    stream.subscribeCandles(instrumentSet.stream().map(InstrumentId::getId).collect(Collectors.toList()), true);


    stopSignalCf.join();
  }
}
