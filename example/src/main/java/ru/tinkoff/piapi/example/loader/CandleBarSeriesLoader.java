package ru.tinkoff.piapi.example.loader;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.num.DecimalNum;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.core.utils.DateUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static ru.tinkoff.piapi.core.utils.MapperUtils.quotationToBigDecimal;

@Getter
@RequiredArgsConstructor
public class CandleBarSeriesLoader {
  private final Duration duration;
  private final BarSeries series;

  public CandleBarSeriesLoader addCandle(Candle candle) {
    var lastCandleEndOpt = Optional.of(series)
      .filter(s -> s.getBarCount() > 0)
      .map(BarSeries::getLastBar)
      .map(Bar::getEndTime);
    var candleTime = ZonedDateTime.ofInstant(DateUtils.timestampToInstant(candle.getTime()), ZoneId.systemDefault());
    if (lastCandleEndOpt.isPresent() && candleTime.isEqual(lastCandleEndOpt.get())) {
      series.addBar(
        new BaseBar(
          duration,
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
        duration,
        candleTime,
        DecimalNum.valueOf(quotationToBigDecimal(candle.getOpen())),
        DecimalNum.valueOf(quotationToBigDecimal(candle.getHigh())),
        DecimalNum.valueOf(quotationToBigDecimal(candle.getLow())),
        DecimalNum.valueOf(quotationToBigDecimal(candle.getClose())),
        DecimalNum.valueOf(candle.getVolume()),
        DecimalNum.valueOf(0)
      );
    }
    return this;
  }

  public CandleBarSeriesLoader addFromHistory(List<HistoricCandle> candles) {
    candles.forEach(historyCandle->series.addBar(
      new BaseBar(
        duration,
        ZonedDateTime.ofInstant(DateUtils.timestampToInstant(historyCandle.getTime()), ZoneId.systemDefault()),
        quotationToBigDecimal(historyCandle.getOpen()),
        quotationToBigDecimal(historyCandle.getHigh()),
        quotationToBigDecimal(historyCandle.getLow()),
        quotationToBigDecimal(historyCandle.getClose()),
        BigDecimal.valueOf(historyCandle.getVolume())
      )
    ));
    return this;
  }
}
