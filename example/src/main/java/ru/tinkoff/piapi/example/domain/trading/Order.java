package ru.tinkoff.piapi.example.domain.trading;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.tinkoff.piapi.contract.v1.OrderType;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.InstrumentId;

import java.util.UUID;

@Getter
public abstract class Order<T extends Order.OrderSpec> {
  private final OrderId id;
  private final OrderType type;
  private final InstrumentId instrumentId;
  private final T spec;

  Order(OrderId id, OrderType type, InstrumentId instrumentId, T spec) {
    this.id = id;
    this.type = type;
    this.instrumentId = instrumentId;
    this.spec = spec;
  }


  public static class LimitOrder extends Order<LimitSpec> {
    public LimitOrder(InstrumentId instrumentId, Quantity quantity, Quantity price) {
      super(new OrderId(UUID.randomUUID().toString()), OrderType.ORDER_TYPE_LIMIT, instrumentId, new LimitSpec(quantity, price));
    }
  }

  public static class MarketOrder extends Order<MarketSpec> {
    public MarketOrder(OrderType type, InstrumentId instrumentId, Quantity quantity) {
      super(new OrderId(UUID.randomUUID().toString()), OrderType.ORDER_TYPE_MARKET, instrumentId, new MarketSpec(quantity));
    }
  }

  @Getter
  @RequiredArgsConstructor
  public static class LimitSpec extends OrderSpec {
    private final Quantity quantity;
    private final Quantity price;
  }

  @Getter
  @RequiredArgsConstructor
  public static class MarketSpec extends OrderSpec {
    private final Quantity quantity;
  }

  public static abstract class OrderSpec {

  }
}
