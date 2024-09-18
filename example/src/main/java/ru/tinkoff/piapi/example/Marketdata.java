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
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Marketdata {

  private final MarketDataService service;

  public List<InstrumentWithPrice> getPrices(Set<Instruments.Instrument> instruments) {
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
          instrument -> new InstrumentWithPrice(
            instrument,
            lpMap.get(instrument.getUuid()),
            cpMap.get(instrument.getUuid())
          )
        );
      })
      .toJavaList();
  }

  public List<InstrumentWithCandles> getCandles(Set<Instruments.Instrument> instruments, CandleInterval interval, Instant from, Instant to) {
    return Stream.ofAll(instruments)
      .sliding(100)
      .flatMap(batch -> batch.map(instrument -> {
        var candles = service.getCandlesSync(instrument.getUuid(), from, to, interval);
        return new InstrumentWithCandles(instrument, candles);
      }))
      .toJavaList();
  }

  public List<InstrumentWithTradingStatus> getTradingStatuses(Set<Instruments.Instrument> instruments) {
    return Stream.ofAll(instruments)
      .sliding(100)
      .flatMap(batch -> batch.map(instrument -> {
        var status = service.getTradingStatusSync(instrument.getUuid());
        return new InstrumentWithTradingStatus(instrument, status.getTradingStatus());
      }))
      .toJavaList();
  }

  @Data
  @RequiredArgsConstructor
  public static class InstrumentWithPrice {
    private final Instruments.Instrument instrument;
    private final LastPrice lastPrice;
    private final InstrumentClosePriceResponse closePrice;
  }

  @Data
  @RequiredArgsConstructor
  public static class InstrumentWithCandles {
    private final Instruments.Instrument instrument;
    private final List<HistoricCandle> candles;
  }

  @Data
  @RequiredArgsConstructor
  public static class InstrumentWithTradingStatus {
    private final Instruments.Instrument instrument;
    private final SecurityTradingStatus tradingStatus;
  }


}
