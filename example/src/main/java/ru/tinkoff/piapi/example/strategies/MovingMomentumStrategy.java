package ru.tinkoff.piapi.example.strategies;

import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.BaseStrategy;
import org.ta4j.core.Rule;
import org.ta4j.core.Strategy;
import org.ta4j.core.TradingRecord;
import org.ta4j.core.backtest.BarSeriesManager;
import org.ta4j.core.criteria.pnl.ReturnCriterion;
import org.ta4j.core.indicators.EMAIndicator;
import org.ta4j.core.indicators.MACDIndicator;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.StochasticOscillatorKIndicator;
import org.ta4j.core.indicators.adx.ADXIndicator;
import org.ta4j.core.indicators.adx.MinusDIIndicator;
import org.ta4j.core.indicators.adx.PlusDIIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.indicators.helpers.HighPriceIndicator;
import org.ta4j.core.indicators.helpers.LowPriceIndicator;
import org.ta4j.core.indicators.helpers.MedianPriceIndicator;
import org.ta4j.core.indicators.numeric.NumericIndicator;
import org.ta4j.core.indicators.pivotpoints.FibonacciReversalIndicator;
import org.ta4j.core.indicators.pivotpoints.PivotPointIndicator;
import org.ta4j.core.indicators.pivotpoints.TimeLevel;
import org.ta4j.core.indicators.statistics.MeanDeviationIndicator;
import org.ta4j.core.num.DecimalNum;
import org.ta4j.core.rules.CrossedDownIndicatorRule;
import org.ta4j.core.rules.CrossedUpIndicatorRule;
import org.ta4j.core.rules.OverIndicatorRule;
import org.ta4j.core.rules.StopLossRule;
import org.ta4j.core.rules.UnderIndicatorRule;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.GetCandlesRequest;
import ru.tinkoff.piapi.core.ApiConfig;
import ru.tinkoff.piapi.core.InvestApi;
import ru.tinkoff.piapi.example.loader.CandleBarSeriesLoader;

import java.time.Duration;
import java.time.Instant;

public class MovingMomentumStrategy {

  /**
   * @param series the bar series
   * @return the moving momentum strategy
   */
  public static Strategy buildStrategy(BarSeries series) {
    if (series == null) {
      throw new IllegalArgumentException("Series cannot be null");
    }

//    final ClosePriceIndicator closePriceIndicator = new ClosePriceIndicator(series);
//    final SMAIndicator smaIndicator = new SMAIndicator(closePriceIndicator, 50);
//
//    final int adxBarCount = 14;
//    final ADXIndicator adxIndicator = new ADXIndicator(series, adxBarCount);
//    final OverIndicatorRule adxOver20Rule = new OverIndicatorRule(adxIndicator, 20);
//
//    final PlusDIIndicator plusDIIndicator = new PlusDIIndicator(series, adxBarCount);
//    final MinusDIIndicator minusDIIndicator = new MinusDIIndicator(series, adxBarCount);
//
//    final Rule plusDICrossedUpMinusDI = new CrossedUpIndicatorRule(plusDIIndicator, minusDIIndicator);
//    final Rule plusDICrossedDownMinusDI = new CrossedDownIndicatorRule(plusDIIndicator, minusDIIndicator);
//    final OverIndicatorRule closePriceOverSma = new OverIndicatorRule(closePriceIndicator, smaIndicator);
//    final Rule entryRule = adxOver20Rule.and(plusDICrossedUpMinusDI).and(closePriceOverSma);
//
//    final UnderIndicatorRule closePriceUnderSma = new UnderIndicatorRule(closePriceIndicator, smaIndicator);
//    final Rule exitRule = adxOver20Rule.and(plusDICrossedDownMinusDI).and(closePriceUnderSma);
//
//    return new BaseStrategy("ADX", entryRule, exitRule, adxBarCount);

    ClosePriceIndicator closePrice = new ClosePriceIndicator(series);
    LowPriceIndicator lowPriceIndicator = new LowPriceIndicator(series);
    HighPriceIndicator highPriceIndicator = new HighPriceIndicator(series);
    MedianPriceIndicator medianPriceIndicator = new MedianPriceIndicator(series);



    PivotPointIndicator pivotPointIndicator = new PivotPointIndicator(series, TimeLevel.BARBASED);
    FibonacciReversalIndicator fibonacciFactor1Indicator = new FibonacciReversalIndicator(pivotPointIndicator, FibonacciReversalIndicator.FibonacciFactor.FACTOR_1, FibonacciReversalIndicator.FibReversalTyp.RESISTANCE);
    FibonacciReversalIndicator fibonacciFactor2Indicator = new FibonacciReversalIndicator(pivotPointIndicator, FibonacciReversalIndicator.FibonacciFactor.FACTOR_2, FibonacciReversalIndicator.FibReversalTyp.SUPPORT);

    var fibonacciFactor2IndicatorDelta = NumericIndicator.of(fibonacciFactor2Indicator)
      .multipliedBy(1.02)
      ;

    // The bias is bullish when the shorter-moving average moves above the longer
    // moving average.
    // The bias is bearish when the shorter-moving average moves below the longer
    // moving average.
    EMAIndicator shortEma = new EMAIndicator(closePrice, 9);
    EMAIndicator longEma = new EMAIndicator(closePrice, 26);

    StochasticOscillatorKIndicator stochasticOscillK = new StochasticOscillatorKIndicator(series, 3);

    MACDIndicator macd = new MACDIndicator(closePrice, 9, 26);
    EMAIndicator emaMacd = new EMAIndicator(macd, 18);

   // var smaStoh = new MeanDeviationIndicator(stochasticOscillK, 3);
    // Entry rule
    Rule entryRule = new OverIndicatorRule(shortEma, longEma) // Trend
      .and(new CrossedDownIndicatorRule(stochasticOscillK, 20)) // Signal 1
      .and(new CrossedDownIndicatorRule(closePrice, fibonacciFactor1Indicator)) // Signal 1
      .and(new OverIndicatorRule(macd, emaMacd)); // Signal 2

    // Exit rule
    Rule exitRule = new UnderIndicatorRule(shortEma, longEma) // Trend
      .and(new CrossedUpIndicatorRule(stochasticOscillK, 80)) // Signal 1
      .and(new OverIndicatorRule(closePrice, fibonacciFactor2Indicator)) // Signal 1
      .and(new UnderIndicatorRule(macd, emaMacd)); // Signal 2

    return new BaseStrategy(entryRule, exitRule);
  }

  public static void main(String[] args) {
    var api = InvestApi.create(ApiConfig
      .loadFromClassPath("example-bot.properties"));

    BarSeries series = new BaseBarSeries("Candle");
    series.setMaximumBarCount(1200);

    var historicCandles = api.getMarketDataService()
      .getCandlesSync(
        "e6123145-9665-43e0-8413-cd61b8aa9b13",
        Instant.now().minusSeconds(24* 3600),
        Instant.now(),
        CandleInterval.CANDLE_INTERVAL_1_MIN
      );

    CandleBarSeriesLoader barSeriesLoader = new CandleBarSeriesLoader(Duration.ofMinutes(1), series)
      .addFromHistory(historicCandles);

    series = barSeriesLoader.getSeries();


    // Building the trading strategy
    Strategy strategy = buildStrategy(series);

    // Running the strategy
    BarSeriesManager seriesManager = new BarSeriesManager(series);
    TradingRecord tradingRecord = seriesManager.run(strategy);
    System.out.println("Number of positions for the strategy: " + tradingRecord.getPositionCount() + tradingRecord.toString());

    // Analysis
    System.out.println("Total profit for the strategy: " + new ReturnCriterion().calculate(series, tradingRecord));
  }
}
