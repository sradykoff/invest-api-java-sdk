package ru.tinkoff.piapi.example.trading;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.OrderDirection;
import ru.tinkoff.piapi.contract.v1.OrderType;
import ru.tinkoff.piapi.contract.v1.PostOrderRequest;
import ru.tinkoff.piapi.contract.v1.PostOrderResponse;
import ru.tinkoff.piapi.core.OrdersService;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.AccountId;
import ru.tinkoff.piapi.example.domain.InstrumentId;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class LiveTradeOrderManager {

  private final OrdersService ordersService;
  private final AccountId accountId;


  public LiveTradeOrderReceipt buyBestPrice(InstrumentId instrumentId, Quantity quantity) {
    String orderId = UUID.randomUUID().toString();
    var postResponse = ordersService.postOrderSync(
      instrumentId.getId(),
      quantity.getValue().longValue(),
      Quantity.ONE.toQuotation(),
      OrderDirection.ORDER_DIRECTION_BUY,
      accountId.getId(),
      OrderType.ORDER_TYPE_BESTPRICE,
      orderId
    );
    return new LiveTradeOrderReceipt(postResponse);
  }

  public LiveTradeOrderReceipt sellBestPrice(InstrumentId instrumentId, Quantity quantity) {
    String orderId = UUID.randomUUID().toString();
    var postResponse = ordersService.postOrderSync(
      instrumentId.getId(),
      quantity.getValue().longValue(),
      Quantity.ONE.toQuotation(),
      OrderDirection.ORDER_DIRECTION_SELL,
      accountId.getId(),
      OrderType.ORDER_TYPE_BESTPRICE,
      orderId
    );
    return new LiveTradeOrderReceipt(postResponse);
  }


  PostOrderRequest.Builder buildPostOrderRequest(
    InstrumentId instrumentId,
    Quantity quantity,
    OrderType orderType,
    OrderDirection orderDirection
  ) {
    return PostOrderRequest.newBuilder()
      .setInstrumentId(instrumentId.getId())
      .setAccountId(accountId.getId())
      .setQuantity(quantity.getValue().longValue())
      .setOrderType(orderType)
      .setDirection(orderDirection)
      ;
  }


  @Getter
  @RequiredArgsConstructor
  public static class LiveTradeOrderReceipt {

    private final PostOrderResponse postOrderResponse;
  }


}
