package ru.tinkoff.piapi.example;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.vavr.control.Try;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
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

  @CommandLine.Command(
    description = "Runs API Examples",
    mixinStandardHelpOptions = true,
    subcommands = {
      SnapshotCommand.class,
      CandlePlot.class
    })
  @RequiredArgsConstructor
  public static class ExampleCommand implements RunnableExample {

    private final ApiExampleCallback callback;

    @Override
    public void run() {
      log.debug("starting examples");
      callback.onInit();
    }
  }

  @CommandLine.Command(name = "snapshot", description = "Runs Snapshot Example")
  public static class SnapshotCommand implements RunnableExample {

    @CommandLine.ParentCommand
    ExampleCommand parentCommand;

    @CommandLine.Option(names = {"--root-path"}, description = "Root path for output files", defaultValue = "./out/")
    String outRootPath;

    @Override
    public void run() {
      log.debug("run snapshot");
      parentCommand.callback.onSnapshot(this);
    }
  }

  @CommandLine.Command(name = "candle-plot", description = "Prints candle plot")
  public static class CandlePlot implements RunnableExample {

    @CommandLine.ParentCommand
    ExampleCommand parentCommand;


    @Override
    public void run() {
      log.debug("run candle plot");
      parentCommand.callback.onCandlePlot(this);
    }
  }

  @CommandLine.Command(name = "run-bot", description = "Runs bot")
  public static class RunBot implements RunnableExample {

    @CommandLine.ParentCommand
    ExampleCommand parentCommand;


    @Override
    public void run() {
      log.debug("run bot");
      parentCommand.callback.onRunBot(this);
    }
  }


  public interface ApiExampleCallback {
    void onInit();

    void onSnapshot(SnapshotCommand cmd);

    void onCandlePlot(CandlePlot candlePlot);

    void onRunBot(RunBot runBot);
  }

  public static CommandLine appCommandLine(ApiExampleCallback callback) {
//    CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.create();
//    spec.mixinStandardHelpOptions(true); // usageHelp and versionHelp options
//    spec.addOption(CommandLine.Model.OptionSpec.builder("-il", "--instruments-limit")
//      .paramLabel("LIMIT")
//      .defaultValue("10")
//      .type(int.class)
//      .description("limit of instruments to get").build());

    // configure a custom handler

    //commandLine.setExecutionStrategy(ExampleUtils::run);

    return new CommandLine(new ExampleCommand(callback))
      .setParameterExceptionHandler(ExampleUtils::invalidUserInput) // configure a custom handler
      .setExecutionExceptionHandler(ExampleUtils::runtimeException);
  }

  static int run(CommandLine.ParseResult pr) {
    // handle requests for help or version information
    Integer helpExitCode = CommandLine.executeHelpRequest(pr);
    if (helpExitCode != null) {
      return helpExitCode;
    }

    // implement the business logic
    int count = pr.matchedOptionValue("il", 10);


    return 1000;
  }

  public enum ExampleCase {

  }

  // custom handler for runtime errors that does not print a stack trace
  static int runtimeException(Exception e,
                              CommandLine commandLine,
                              CommandLine.ParseResult parseResult) {
    commandLine.getErr().println("INTERNAL ERROR: " + e.getMessage());
    return CommandLine.ExitCode.SOFTWARE;
  }

  // custom handler for invalid input that does not print usage help
  static int invalidUserInput(CommandLine.ParameterException e, String[] strings) {
    CommandLine commandLine = e.getCommandLine();
    commandLine.getErr().println("ERROR: " + e.getMessage());
    commandLine.getErr().println("Try '"
      + commandLine.getCommandSpec().qualifiedName()
      + " --help' for more information.");
    return CommandLine.ExitCode.USAGE;
  }
}
