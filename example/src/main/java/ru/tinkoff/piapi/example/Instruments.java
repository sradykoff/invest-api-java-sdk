package ru.tinkoff.piapi.example;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import ru.tinkoff.piapi.contract.v1.Asset;
import ru.tinkoff.piapi.contract.v1.AssetType;
import ru.tinkoff.piapi.contract.v1.Bond;
import ru.tinkoff.piapi.contract.v1.Currency;
import ru.tinkoff.piapi.contract.v1.Etf;
import ru.tinkoff.piapi.contract.v1.Future;
import ru.tinkoff.piapi.contract.v1.GetAssetFundamentalsResponse;
import ru.tinkoff.piapi.contract.v1.InstrumentResponse;
import ru.tinkoff.piapi.contract.v1.Option;
import ru.tinkoff.piapi.contract.v1.Share;
import ru.tinkoff.piapi.core.InstrumentsService;
import ru.tinkoff.piapi.core.InvestApi;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ru.tinkoff.piapi.example.ExampleUtils.writeMessagesToJsonFile;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.*;


@Slf4j
@RequiredArgsConstructor
public class Instruments {

  private final InstrumentsService instrumentsService;

  public InstrumentsList tradeAvailableInstruments() {
    var assets = instrumentsService.getAssetsSync();
    var shares = instrumentsService.getTradableSharesSync();
    var etfs = instrumentsService.getTradableEtfsSync();
    var bonds = instrumentsService.getTradableBondsSync();
    var currencies = instrumentsService.getTradableCurrenciesSync();
    var futures = instrumentsService.getTradableFuturesSync();
    var options = instrumentsService.getTradableOptionsSync();
    return new InstrumentsList(assets, shares, etfs, bonds, currencies, futures, options);
  }


  public InstrumentResponse findInstrument(String query) {
    var isUuid = Try.of(() -> UUID.fromString(query).toString().equals(query))
      .getOrElse(false);

    if (isUuid) {
      return instrumentsService.getInstrumentByUIDSync(query);
    }

    var instrument = instrumentsService.findInstrument(query);

    return null;
  }


  public List<GetAssetFundamentalsResponse.StatisticResponse> assetFundamentsls(Set<String> assetUids) {
    return Stream.ofAll(assetUids)
      .sliding(99)
      .map(sharesChunk -> instrumentsService
        .getAssetFundamentalsSync(
          sharesChunk.toJavaStream()
            .collect(Collectors.toSet())
        ))
      .toJavaStream()
      .flatMap(resp -> resp.getFundamentalsList().stream())
      .sorted(Comparator.comparing(GetAssetFundamentalsResponse.StatisticResponse::getAssetUid))
      .collect(Collectors.toList());

  }


  public void tradingSchedules() {
//    var tradingSchedules = instrumentsService.getTradingSchedulesSync(from, to);
  }


  @RequiredArgsConstructor
  public static class InstrumentsList {
    final List<Asset> assets;
    final List<Share> shares;
    final List<Etf> etfs;
    final List<Bond> bonds;
    final List<Currency> currencies;
    final List<Future> futures;
    final List<Option> options;
  }

  public enum InstrumentObjType {
    ASSET,
    SHARE,
    ETF,
    BOND,
    CURRENCY,
    FUTURE,
    OPTION;
  }

  @Data
  @RequiredArgsConstructor
  public static class Instrument {
    private final String uuid;
    private final InstrumentObjType type;
    private final Object data;
  }

  private static Map<InstrumentObjType, Function<InstrumentsList, List<Message>>> INSTRUMENT_OBJ_UID_MAPPER =
    HashMap.of(
      ASSET, instruments -> instruments.assets.stream()
        .sorted(Comparator.comparing(Asset::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList()),
      SHARE, instruments -> instruments.shares.stream()
        .sorted(Comparator.comparing(Share::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList()),
      ETF, instruments -> instruments.etfs.stream()
        .sorted(Comparator.comparing(Etf::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList()),
      BOND, instruments -> instruments.bonds.stream()
        .sorted(Comparator.comparing(Bond::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList()),
      CURRENCY, instruments -> instruments.currencies.stream()
        .sorted(Comparator.comparing(Currency::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList()),
      FUTURE, instruments -> instruments.futures.stream()
        .sorted(Comparator.comparing(Future::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList()),
      OPTION, instruments -> instruments.options.stream()
        .sorted(Comparator.comparing(Option::getUid))
        .map(s -> (Message) s)
        .collect(Collectors.toList())
    );


  public void saveTradingInstrument(String rootPath) {
    var instrumentsData = tradeAvailableInstruments();

    INSTRUMENT_OBJ_UID_MAPPER
      .map(instrumentObjTypeTuple -> instrumentObjTypeTuple.apply((objType, idGetter) -> {
        var messages = idGetter.apply(instrumentsData);
        return Tuple.of(
          objType,
          writeMessagesToJsonFile(messages, rootPath, objType.name().toLowerCase() + ".json")
            .toEither()
        );
      })).forEach(tuple -> tuple.apply((objType, either) -> {
        if (either.isRight()) {
          log.info("saved {} to file:{}", objType, either.get());
        } else
          log.error("error saving {}", objType, either.getLeft());
        return null;
      }));


    var afResponses = assetFundamentsls(
      java.util.stream.Stream.concat(
          instrumentsData.shares.stream()
            .filter(Share::getLiquidityFlag)
            .filter(share -> !share.getForQualInvestorFlag())
            .filter(Share::getBuyAvailableFlag)
            .filter(Share::getSellAvailableFlag)
            .filter(Share::getShortEnabledFlag)
            .map(Share::getAssetUid),
          instrumentsData.bonds.stream()
            .filter(Bond::getLiquidityFlag)
            .filter(share -> !share.getForQualInvestorFlag())
            .filter(Bond::getBuyAvailableFlag)
            .filter(Bond::getSellAvailableFlag)
            .filter(Bond::getShortEnabledFlag)
            .map(Bond::getAssetUid)
        )

        .collect(Collectors.toSet())
    );

    var asfSaveSuccess = writeMessagesToJsonFile(afResponses, rootPath, "asset_fundamentals.json")
      .onFailure(error -> log.info("error saving asset fundamentals", error))
      .toEither();

    if (asfSaveSuccess.isRight()) {
      log.info("saved asset_fundamentals to file:{}", asfSaveSuccess.get());
    }


  }

}
