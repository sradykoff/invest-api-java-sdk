package ru.tinkoff.piapi.example;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.core.stream.MarketDataStreamService;
import ru.tinkoff.piapi.core.stream.StreamProcessor;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class MarketdataStreams {
  private final MarketDataStreamService streamService;

  public List<StreamView> streamInstrument(String nm, Set<Tuple2<Instruments.Instrument, Consumer<MarketDataResponse>>> instruments) {

    return Stream.ofAll(instruments)
      .sliding(100)
      .flatMap(batch -> batch.map(instrumentWithConsumer -> {
        var service = streamService.newStream(nm + instrumentWithConsumer._1.getUuid(), new StreamProcessor<MarketDataResponse>() {
          @Override
          public void process(MarketDataResponse response) {
            if (instrumentWithConsumer._2 != null) instrumentWithConsumer._2.accept(response);
          }
        }, error -> log.error("stream error", error));

        return (StreamView)new StreamViewImpl(instrumentWithConsumer._1);
      }))
        .toJavaList();


    service.subscribeCandles();
    service.subscribeLastPrices();
    service.subscribeTrades();

    service.subscribeOrderbook();

  }

  @RequiredArgsConstructor
  static class StreamViewImpl implements StreamView {
    private final Instruments.Instrument instrument;
    private final MarketDataStr

    @Override
    public Instruments.Instrument instrument() {
      return instrument;
    }
  }

  public interface StreamView {
    Instruments.Instrument instrument();

  }
}
