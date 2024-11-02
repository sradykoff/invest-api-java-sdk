package ru.tinkoff.piapi.example;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.core.utils.DateUtils;
import ru.tinkoff.piapi.core.utils.MapperUtils;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.ScatterTrace;
import tech.tablesaw.plotly.traces.Trace;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ExampleUtils {

  public static <K extends MessageOrBuilder> Try<Path> writeMessagesToJsonFile(List<K> messages, String rootPath, String fileName) {
    return writeMessagesToJsonFile(messages, rootPath, fileName, Function.identity());
  }

  public static <T, K extends MessageOrBuilder> Try<Path> writeMessagesToJsonFile(List<T> messages, String rootPath, String fileName, Function<T, K> mapper) {
    var path = Paths.get(rootPath, fileName);
    return Try.of(() -> {
        Files.writeString(
          path,
          messages.stream()
            .map(message -> Try.of(() -> JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .preservingProtoFieldNames()
                .print(mapper.apply(message)))
              .onFailure(error -> log.error("json  print", error))
            )
            .filter(Try::isSuccess)
            .map(Try::get)
            .collect(Collectors.joining("\n"))
        );
        return path;
      }
    );
  }


  public static void showOHLCPlot(List<Marketdata.InstrumentWithPrice> mdAllPrices) {
    mdAllPrices
      .stream()
      .filter(instrumentWithPrice -> !instrumentWithPrice.getCandles().isEmpty())
      .forEach(instrumentWithPrice -> {
        var dateTimeColumn = InstantColumn.create("ts", instrumentWithPrice.getCandles().stream()
          .map(HistoricCandle::getTime)
          .map(DateUtils::timestampToInstant)
          .collect(Collectors.toList()));
        var openPriceColumn = DoubleColumn.create("open", instrumentWithPrice.getCandles().stream()
          .map(HistoricCandle::getOpen)
          .map(MapperUtils::quotationToBigDecimal)
          .map(BigDecimal::doubleValue)
          .collect(Collectors.toList()));
        var closePriceColumn = DoubleColumn.create("close", instrumentWithPrice.getCandles().stream()
          .map(HistoricCandle::getClose)
          .map(MapperUtils::quotationToBigDecimal)
          .map(BigDecimal::doubleValue)
          .collect(Collectors.toList()));
        var highPriceColumn = DoubleColumn.create("high", instrumentWithPrice.getCandles().stream()
          .map(HistoricCandle::getHigh)
          .map(MapperUtils::quotationToBigDecimal)
          .map(BigDecimal::doubleValue)
          .collect(Collectors.toList()));
        var lowPriceColumn = DoubleColumn.create("low", instrumentWithPrice.getCandles().stream()
          .map(HistoricCandle::getLow)
          .map(MapperUtils::quotationToBigDecimal)
          .map(BigDecimal::doubleValue)
          .collect(Collectors.toList()));

        var table = Table.create("candles", dateTimeColumn, openPriceColumn, closePriceColumn, highPriceColumn, lowPriceColumn);

        var instrumentUid = instrumentWithPrice.getInstrument().getUuid();
        table.write().csv("candle_" + instrumentUid + ".csv");

        //Plot.show(CandlestickPlot.create("Prices" + instrumentUid, table, "ts","open", "high", "low", "close"));


        Layout layout = Layout.builder("Prices" + instrumentUid, "ts").build();
        Column<?> x = table.column("ts");
        NumericColumn<?> open = table.numberColumn("open");
        NumericColumn<?> high = table.numberColumn("high");
        NumericColumn<?> low = table.numberColumn("low");
        NumericColumn<?> close = table.numberColumn("close");
        ScatterTrace trace;
        trace = ScatterTrace.builder(table.instantColumn("ts"), open, high, low, close).type("candlestick").build();

        var f = new Figure(layout, new Trace[]{trace});
        Plot.show(f);
      });
  }


  public static void printPortfolioBalance(Operations.EPortfolio portfolio) {

  }

  interface RunnableExample extends Runnable {

  }
}
