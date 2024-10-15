package ru.tinkoff.piapi.example;

import com.google.protobuf.Message;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.Asset;
import ru.tinkoff.piapi.contract.v1.Bond;
import ru.tinkoff.piapi.contract.v1.Currency;
import ru.tinkoff.piapi.contract.v1.Etf;
import ru.tinkoff.piapi.contract.v1.Future;
import ru.tinkoff.piapi.contract.v1.GetAssetFundamentalsResponse;
import ru.tinkoff.piapi.contract.v1.Option;
import ru.tinkoff.piapi.contract.v1.Share;
import ru.tinkoff.piapi.contract.v1.TradingSchedule;
import ru.tinkoff.piapi.core.InstrumentsService;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ru.tinkoff.piapi.example.ExampleUtils.writeMessagesToJsonFile;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.ASSET;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.BOND;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.CURRENCY;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.ETF;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.FUTURE;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.OPTION;
import static ru.tinkoff.piapi.example.Instruments.InstrumentObjType.SHARE;


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
    var tradingSchedules = instrumentsService.getTradingSchedulesSync();

    var afResponses = assetFundamentsls(
      Stream.ofAll(
          shares.stream()
            .filter(Share::getLiquidityFlag)
            .filter(share -> !share.getForQualInvestorFlag())
            .filter(Share::getBuyAvailableFlag)
            .filter(Share::getSellAvailableFlag)
            .filter(Share::getShortEnabledFlag)
            .map(Share::getAssetUid)
        )
        .appendAll(
          Stream.ofAll(
            bonds.stream()
              .filter(Bond::getLiquidityFlag)
              .filter(share -> !share.getForQualInvestorFlag())
              .filter(Bond::getBuyAvailableFlag)
              .filter(Bond::getSellAvailableFlag)
              .filter(Bond::getShortEnabledFlag)
              .map(Bond::getAssetUid)
          )
        )
//        .appendAll(
//          Stream.ofAll(
//            etfs.stream()
//              .filter(Etf::getLiquidityFlag)
//              .filter(share -> !share.getForQualInvestorFlag())
//              .filter(Etf::getBuyAvailableFlag)
//              .filter(Etf::getSellAvailableFlag)
//              .filter(Etf::getShortEnabledFlag)
//              .map(Etf::getAssetUid)
//          )
//        )
        .toJavaSet()

    );
    return new InstrumentsList(assets, shares, etfs, bonds, currencies, futures, options, afResponses, tradingSchedules);
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

  @RequiredArgsConstructor
  public static class InstrumentsList {
    final List<Asset> assets;
    final List<Share> shares;
    final List<Etf> etfs;
    final List<Bond> bonds;
    final List<Currency> currencies;
    final List<Future> futures;
    final List<Option> options;
    final List<GetAssetFundamentalsResponse.StatisticResponse> assetFundamentals;
    final List<TradingSchedule> tradingSchedules;

    public List<Instrument> allInstruments() {
      var assetMap = assets.stream().collect(Collectors.toMap(Asset::getUid, Function.identity(), (k1, k2) -> k1));
      var assetFundamentalsMap = assetFundamentals.stream().collect(Collectors.toMap(GetAssetFundamentalsResponse.StatisticResponse::getAssetUid, Function.identity(), (k1, k2) -> k1));
      return Stream.ofAll(shares)
        .map(share -> new Instrument(
          share.getUid(),
          assetMap.get(share.getAssetUid()),
          InstrumentObjType.SHARE,
          share,
          assetFundamentalsMap.get(share.getAssetUid())
        ))
        .appendAll(
          Stream.ofAll(etfs)
            .map(etf -> new Instrument(
              etf.getUid(),
              assetMap.get(etf.getAssetUid()),
              InstrumentObjType.ETF,
              etf,
              assetFundamentalsMap.get(etf.getAssetUid())
            ))
        )
        .appendAll(
          Stream.ofAll(bonds)
            .map(bond -> new Instrument(
              bond.getUid(),
              assetMap.get(bond.getAssetUid()),
              InstrumentObjType.BOND,
              bond,
              assetFundamentalsMap.get(bond.getAssetUid())
            ))
        )
        .appendAll(
          Stream.ofAll(currencies)
            .map(currency -> new Instrument(
              currency.getUid(),
              null,
              InstrumentObjType.CURRENCY,
              currency,
              null
            ))
        )
        .appendAll(
          Stream.ofAll(futures)
            .map(future -> new Instrument(
              future.getUid(),
              assetMap.get(future.getBasicAsset()),
              InstrumentObjType.FUTURE,
              future,
              assetFundamentalsMap.get(future.getBasicAsset())
            ))

        )
        .appendAll(
          Stream.ofAll(options)
            .map(option -> new Instrument(
              option.getUid(),
              assetMap.get(option.getBasicAsset()),
              InstrumentObjType.OPTION,
              option,
              assetFundamentalsMap.get(option.getBasicAsset()))
            )
        )
        .toJavaList();
    }

    public List<Instrument> select(InstrumentObjType type, int limit) {
      var assetComparator = Comparator.comparingDouble(GetAssetFundamentalsResponse.StatisticResponse::getAverageDailyVolumeLast4Weeks)
        .thenComparingDouble(GetAssetFundamentalsResponse.StatisticResponse::getRoe)
        .thenComparingDouble(GetAssetFundamentalsResponse.StatisticResponse::getOneYearAnnualRevenueGrowthRate);

      return allInstruments()
        .stream()
        .filter(instrument -> instrument.type == type)
        .sorted(Comparator.comparing(Instrument::getAssetFundamentals, Comparator.nullsLast(assetComparator)))
        .limit(limit)
        .collect(Collectors.toList());
    }
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
    private final Asset asset;
    private final InstrumentObjType type;
    private final Object rawData;
    private final GetAssetFundamentalsResponse.StatisticResponse assetFundamentals;
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


  public void saveTradingInstrument(InstrumentsList instrumentsData, String rootPath) {

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


    var afResponses = instrumentsData.assetFundamentals;

    var asfSaveSuccess = writeMessagesToJsonFile(afResponses, rootPath, "asset_fundamentals.json")
      .onFailure(error -> log.info("error saving asset fundamentals", error))
      .toEither();

    if (asfSaveSuccess.isRight()) {
      log.info("saved asset_fundamentals to file:{}", asfSaveSuccess.get());
    }

    var tradingSchedulers = instrumentsData.tradingSchedules;

    var tradingSchedulersSaveSuccess = writeMessagesToJsonFile(tradingSchedulers, rootPath, "trading_schedules.json")
      .onFailure(error -> log.info("error saving trading schedules", error))
      .toEither();

    if (tradingSchedulersSaveSuccess.isRight()) {
      log.info("saved trading schedules to file:{}", tradingSchedulersSaveSuccess.get());
    }


  }

}
