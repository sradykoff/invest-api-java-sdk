package ru.tinkoff.piapi.example;

import io.vavr.collection.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.InstrumentClosePriceResponse;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.SecurityTradingStatus;
import ru.tinkoff.piapi.core.MarketDataService;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Marketdata {

  private final MarketDataService service;

  public List<InstrumentWithPrice> getPricesAndCandles(Set<Instruments.Instrument> instruments) {
    var candleFrom = Instant.now().minus(1, ChronoUnit.HOURS);
    var candleTo = Instant.now();
    return Stream.ofAll(instruments)
      .sliding(100)
      .flatMap(batch -> {
        var ids = batch.map(Instruments.Instrument::getUuid).toJavaSet();
        var lastPricesResp = service.getLastPricesSync(ids);
        var closePricesResp = service.getClosePricesSync(ids);

        var lpMap = lastPricesResp.stream()
          .collect(Collectors.toMap(LastPrice::getInstrumentUid, Function.identity(), (a, b) -> a));
        var cpMap = closePricesResp.stream()
          .collect(Collectors.toMap(InstrumentClosePriceResponse::getInstrumentUid, Function.identity(), (a, b) -> a));
        return batch.map(
          instrument -> {
            var status = service.getTradingStatusSync(instrument.getUuid());
            var candles = service.getCandlesSync(instrument.getUuid(), candleFrom, candleTo, CandleInterval.CANDLE_INTERVAL_1_MIN);
            return new InstrumentWithPrice(
              instrument,
              lpMap.get(instrument.getUuid()),
              cpMap.get(instrument.getUuid()),
              candles,
              status.getTradingStatus()
            );
          }
        );
      })
      .toJavaList();
  }


  @Data
  @RequiredArgsConstructor
  public static class InstrumentWithPrice {
    private final Instruments.Instrument instrument;
    private final LastPrice lastPrice;
    private final InstrumentClosePriceResponse closePrice;
    private final List<HistoricCandle> candles;
    private final SecurityTradingStatus tradingStatus;
  }


}
