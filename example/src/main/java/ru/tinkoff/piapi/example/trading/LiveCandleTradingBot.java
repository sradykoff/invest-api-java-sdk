package ru.tinkoff.piapi.example.trading;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.ta4j.core.Strategy;
import org.ta4j.core.TradingRecord;
import org.ta4j.core.num.DecimalNum;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.bars.CandleBarSeries;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.trading.OrderTradeDirection;

import java.util.concurrent.locks.StampedLock;

@RequiredArgsConstructor
@Getter
public class LiveCandleTradingBot {
  private final InstrumentId instrumentId;
  private final CandleBarSeries candleBarSeries;
  private final Strategy strategy;
  private final TradingRecord tradingRecord;
  private final StampedLock lock = new StampedLock();


  public LiveTradeSignal signalOnCandle(Candle candle) {
    long stamp = lock.writeLock();
    try {
      var series = candleBarSeries.addCandle(candle)
              .getSeries();
      var endIndex = series.getEndIndex();
      if (strategy.shouldEnter(endIndex, tradingRecord)) {
        return new LiveTradeSignal(endIndex, instrumentId, OrderTradeDirection.BUY, Quantity.ofQuotation(candle.getClose()));
      } else if (strategy.shouldExit(endIndex, tradingRecord)) {
        return new LiveTradeSignal(endIndex, instrumentId, OrderTradeDirection.SELL, Quantity.ofQuotation(candle.getClose()));
      } else {
        return new LiveTradeSignal(endIndex, instrumentId, OrderTradeDirection.HOLD, Quantity.ofQuotation(candle.getClose()));
      }
    } finally {
      lock.unlockWrite(stamp);
    }

  }

  public LiveCandleTradingBot onEnter(LiveTradeSignal signal, Quantity price, Quantity quantity) {
    long stamp = lock.writeLock();
    try {
      tradingRecord.enter(signal.index, DecimalNum.valueOf(price.getValue()), DecimalNum.valueOf(quantity.getValue()));
    } finally {
      lock.unlockWrite(stamp);
    }
    return this;
  }

  public LiveCandleTradingBot onExit(LiveTradeSignal signal, Quantity price, Quantity quantity) {
    long stamp = lock.writeLock();
    try {
      tradingRecord.exit(signal.index, DecimalNum.valueOf(price.getValue()), DecimalNum.valueOf(quantity.getValue()));
    } finally {
      lock.unlockWrite(stamp);
    }
    return this;
  }


  @RequiredArgsConstructor
  @Data
  public static class LiveTradeSignal {
    final int index;
    final InstrumentId instrumentId;
    final OrderTradeDirection direction;
    final Quantity closePrice;

    public boolean isBuy() {
      return direction == OrderTradeDirection.BUY;
    }

    public boolean isSell() {
      return direction == OrderTradeDirection.SELL;
    }

    public boolean isHold() {
      return direction == OrderTradeDirection.HOLD;
    }
  }


}
