package ru.tinkoff.piapi.example.trading;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.OrderDirection;
import ru.tinkoff.piapi.contract.v1.OrderExecutionReportStatus;
import ru.tinkoff.piapi.contract.v1.OrderType;
import ru.tinkoff.piapi.contract.v1.PostOrderRequest;
import ru.tinkoff.piapi.contract.v1.PostOrderResponse;
import ru.tinkoff.piapi.core.OrdersService;
import ru.tinkoff.piapi.core.models.Quantity;
import ru.tinkoff.piapi.example.domain.AccountId;
import ru.tinkoff.piapi.example.domain.InstrumentId;

import java.util.Set;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class LiveTradeOrderManager {

    private static final Set<OrderExecutionReportStatus> SUCCESS_STATUS = Set.of(
            OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW,
            OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL,
            OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL
    );
    private final OrdersService ordersService;
    private final AccountId accountId;


    public LiveTradeOrderReceipt buyLimit(InstrumentId instrumentId, Quantity quantity, Quantity closePrice) {
        String orderId = UUID.randomUUID().toString();
        try {
            var postResponse = ordersService.postOrderSync(
                    instrumentId.getId(),
                    quantity.getValue().longValue(),
                    closePrice.toQuotation(),
                    OrderDirection.ORDER_DIRECTION_BUY,
                    accountId.getId(),
                    OrderType.ORDER_TYPE_LIMIT,
                    orderId
            );
            return new LiveTradeOrderReceipt(
                    SUCCESS_STATUS.contains(postResponse.getExecutionReportStatus()),
                    postResponse);
        } catch (Exception e) {
            log.error("Error while buying best price", e);
            return new LiveTradeOrderReceipt(false, PostOrderResponse.newBuilder().build());
        }

    }

    public LiveTradeOrderReceipt sellLimit(InstrumentId instrumentId, Quantity quantity, Quantity closePrice) {
        String orderId = UUID.randomUUID().toString();
        try {
            var postResponse = ordersService.postOrderSync(
                    instrumentId.getId(),
                    quantity.getValue().longValue(),
                    closePrice.toQuotation(),
                    OrderDirection.ORDER_DIRECTION_SELL,
                    accountId.getId(),
                    OrderType.ORDER_TYPE_LIMIT,
                    orderId
            );
            return new LiveTradeOrderReceipt(
                    SUCCESS_STATUS.contains(postResponse.getExecutionReportStatus()),
                    postResponse
            );
        } catch (Exception e) {
            log.error("Error while selling best price", e);
            return new LiveTradeOrderReceipt(false, PostOrderResponse.newBuilder().build());
        }

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

        private final boolean success;
        private final PostOrderResponse postOrderResponse;
    }


}
