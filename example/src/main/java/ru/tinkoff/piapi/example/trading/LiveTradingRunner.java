package ru.tinkoff.piapi.example.trading;

import lombok.extern.slf4j.Slf4j;
import org.ta4j.core.BaseTradingRecord;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.core.ApiConfig;
import ru.tinkoff.piapi.core.InvestApi;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.core.stream.StreamProcessor;
import ru.tinkoff.piapi.example.bars.CandleBarSeries;
import ru.tinkoff.piapi.example.domain.AccountId;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.portfolio.PositionId;
import ru.tinkoff.piapi.example.strategies.MovingMomentumStrategy;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class LiveTradingRunner {
  private final InvestApi api;
  private final ConcurrentMap<InstrumentId, LiveTradingBot> bots = new ConcurrentHashMap<>();

  public LiveTradingRunner(Set<InstrumentId> instrumentIds, InvestApi api, AccountId accountId) {
    this.api = api;
    var orderManager = new LiveTradeOrderManager(api.getOrdersService(), accountId);
    instrumentIds.forEach(instrumentId -> {
      var cbSeries = new CandleBarSeries(
        instrumentId.toString(),
        Duration.ofSeconds(5),
        720
      );
      bots.put(instrumentId, new LiveTradingBot(
        instrumentId,
        orderManager,
        MovingMomentumStrategy.buildStrategy(cbSeries.getSeries()),
        new BaseTradingRecord(),
        Quantity.ofUnits(5),
        cbSeries
      ));
    });
  }


  public void onTrade(Trade trade) {
    var instrumentId = new InstrumentId(trade.getInstrumentUid());
    var bot = bots.get(instrumentId);
    bot.onTrade(trade);
  }

  public void onOrderBook(ru.tinkoff.piapi.contract.v1.OrderBook orderBook) {
    var instrumentId = new InstrumentId(orderBook.getInstrumentUid());
    var bot = bots.get(instrumentId);
    bot.onOrderBook(orderBook);
  }


  public static void main(String[] args) {
    var api = InvestApi.create(ApiConfig
      .loadFromClassPath("example-bot.properties"));

    var stopSignalCf = new CompletableFuture<>();

    var instrumentSet = Set.of(
      new InstrumentId("e6123145-9665-43e0-8413-cd61b8aa9b13"),
      new InstrumentId("46ae47ee-f409-4776-bf20-43a040b9e7fb"),
      new InstrumentId("0a55e045-e9a6-42d2-ac55-29674634af2f"),
      new InstrumentId("87db07bc-0e02-4e29-90bb-05e8ef791d7b"),
      new InstrumentId("07c7938b-a44a-4b7c-b70c-afada7c37152"),
      new InstrumentId("77cb416f-a91e-48bd-8083-db0396c61a41")

    );

    var tradeBotRunner = new LiveTradingRunner(instrumentSet, api, new AccountId("2147842561"));

    var stream = api.getMarketDataStreamService().newStream("MD-stream", new StreamProcessor<MarketDataResponse>() {
      @Override
      public void process(MarketDataResponse response) {
        if (response.hasTrade()) tradeBotRunner.onTrade(response.getTrade());
        else if (response.hasOrderbook()) tradeBotRunner.onOrderBook(response.getOrderbook());
      }
    }, error -> {
      log.error("stream error", error);
      stopSignalCf.completeExceptionally(error);
    });


    stream.subscribeTrades(
      instrumentSet.stream().map(InstrumentId::getId).collect(Collectors.toList())
    );
    stream.subscribeOrderbook(
      instrumentSet.stream().map(InstrumentId::getId).collect(Collectors.toList()),
      10
    );

    stopSignalCf.join();
  }

}


