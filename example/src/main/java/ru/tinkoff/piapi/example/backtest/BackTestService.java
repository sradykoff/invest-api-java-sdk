package ru.tinkoff.piapi.example.backtest;

import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.example.domain.InstrumentId;
import ru.tinkoff.piapi.example.domain.backtest.OrderBook;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class BackTestService {


  private final ConcurrentMap<InstrumentId, AtomicReference<OrderBook>> orderBooks = new ConcurrentHashMap<>();




}
