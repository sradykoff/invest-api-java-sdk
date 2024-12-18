package ru.tinkoff.piapi.example.trading;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.ta4j.core.Strategy;
import org.ta4j.core.TradingRecord;
import org.ta4j.core.num.DecimalNum;
import ru.tinkoff.piapi.contract.v1.PortfolioResponse;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.bars.CandleBarSeries;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.orderbook.OrderBookState;
import ru.tinkoff.piapi.example.domain.portfolio.Portfolio;
import ru.tinkoff.piapi.example.domain.trading.OrderTradeDirection;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

@Slf4j
@RequiredArgsConstructor
@Getter
public class LiveTradingBot {
    private static final int DEFAULT_MAX_THINK_MS = 5_000;
    private static final int MAX_SIGNAL_DEPTH = 10;
    private final InstrumentId instrumentId;
    private final CandleBarSeries candleBarSeries;
    private final Strategy candleStrategy;
    private final TradingRecord tradingRecord;
    private final StampedLock lock = new StampedLock();
    private final LiveTradeOrderManager orderManager;
    private final AtomicReference<List<LiveSignalState>> candleStatesRef = new AtomicReference<>(List.empty());
    private final AtomicReference<OrderBookState> orderBookStateRef = new AtomicReference<>(new OrderBookState(List.empty()));
    private final AtomicReference<InternalState> internalStateRef = new AtomicReference<>();

    public LiveTradingBot(
            InstrumentId instrumentId,
            LiveTradeOrderManager orderManager,
            Strategy candleStrategy,
            TradingRecord tradingRecord,
            Quantity tradeableQuantity,
            CandleBarSeries candleBarSeries) {
        this.instrumentId = instrumentId;
        this.candleStrategy = candleStrategy;
        this.tradingRecord = tradingRecord;
        this.candleBarSeries = candleBarSeries;
        this.orderManager = orderManager;
        this.internalStateRef.set(new InternalState(tradeableQuantity));
    }

    public void onTrade(Trade trade) {
        var tradePrice = Quantity.ofQuotation(trade.getPrice());
        long stamp = lock.writeLock();
        try {
            var series = candleBarSeries.addTrade(trade)
                    .getSeries();
            var endIndex = series.getEndIndex();
            if (candleStrategy.shouldEnter(endIndex, tradingRecord)) {
                candleStatesRef.updateAndGet(candleStates -> candleStates.append(
                                new LiveSignalState(endIndex, instrumentId, OrderTradeDirection.BUY, tradePrice))
                        .take(MAX_SIGNAL_DEPTH)
                );
            } else if (candleStrategy.shouldExit(endIndex, tradingRecord)) {
                candleStatesRef.updateAndGet(candleStates -> candleStates.append(
                                new LiveSignalState(endIndex, instrumentId, OrderTradeDirection.SELL, tradePrice))
                        .take(MAX_SIGNAL_DEPTH));
            } else {
                candleStatesRef.updateAndGet(candleStates -> candleStates.append(
                                new LiveSignalState(endIndex, instrumentId, OrderTradeDirection.HOLD, tradePrice))
                        .take(MAX_SIGNAL_DEPTH));
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void onOrderBook(ru.tinkoff.piapi.contract.v1.OrderBook orderBook) {
        var orderBookStateVal = orderBookStateRef.getAndUpdate(orderBookState -> orderBookState.onOrderBook(orderBook));
        think(orderBookStateVal, candleStatesRef.get());
    }


    void think(OrderBookState orderBookState, List<LiveSignalState> candleStates) {
        var sellSignalOpt = candleStates.find(LiveSignalState::isSell);
        var buySignalOpt = candleStates.find(LiveSignalState::isBuy);
        var holdSignalOpt = candleStates.find(LiveSignalState::isHold);
        var headSignalOpt = candleStates.headOption();

//        var buyPriceChangeOpt = orderBookState.averageBuyPricePercentChange();
//        var sellPriceChangeOpt = orderBookState.averageSellPricePercentChange();
//        log.info("bot think head signal:{} book price change buy:{} sell:{} top_order_buy:{} top_order_sell:{} buy_vol:{} sell_vol:{}",
//                headSignalOpt,
//                buyPriceChangeOpt.stream().mapToObj(BigDecimal::valueOf).findAny(),
//                sellPriceChangeOpt.stream().mapToObj(BigDecimal::valueOf).findAny(),
//                orderBookState.queryLastStats().headOption().map(Tuple2::_2)
//                        .map(stats -> stats.topBuyPrices(10).orElse(List.empty())),
//                orderBookState.queryLastStats().headOption().map(Tuple2::_2)
//                        .map(stats -> stats.topSellPrices(10).orElse(List.empty())),
//                orderBookState.queryLastStats().headOption().map(Tuple2::_2)
//                        .map(OrderBookState.OrderBookStats::totalBuyVolume),
//                orderBookState.queryLastStats().headOption().map(Tuple2::_2)
//                        .map(OrderBookState.OrderBookStats::totalSellVolume)
//        );
    }

    public LiveTradingBot onEnter(LiveSignalState signal, Quantity price, Quantity quantity) {
        long stamp = lock.writeLock();
        try {
            tradingRecord.enter(signal.candleIndex, DecimalNum.valueOf(price.getValue()), DecimalNum.valueOf(quantity.getValue()));
        } finally {
            lock.unlockWrite(stamp);
        }
        return this;
    }

    public LiveTradingBot onExit(LiveSignalState signal, Quantity price, Quantity quantity) {
        long stamp = lock.writeLock();
        try {
            tradingRecord.exit(signal.candleIndex, DecimalNum.valueOf(price.getValue()), DecimalNum.valueOf(quantity.getValue()));
        } finally {
            lock.unlockWrite(stamp);
        }
        return this;
    }


    @RequiredArgsConstructor
    @Getter
    @ToString
    public static class LiveSignalState {
        final LocalDateTime created = LocalDateTime.now();
        final int candleIndex;
        final InstrumentId instrumentId;
        final OrderTradeDirection direction;
        final Quantity price;

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

    @RequiredArgsConstructor
    @Getter
    static class InternalState {

        private final Quantity tradeableQuantity;
    }

}
