package ru.tinkoff.piapi.example;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.Account;
import ru.tinkoff.piapi.contract.v1.GetOperationsByCursorResponse;
import ru.tinkoff.piapi.core.OperationsService;
import ru.tinkoff.piapi.core.UsersService;
import ru.tinkoff.piapi.core.models.Portfolio;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class Operations {

  private final OperationsService operationsService;
  private final UsersService usersService;


  public EPortfolio loadPortfolio(List<Account> userAccounts) {
    var now = Instant.now();
    return new EPortfolio(userAccounts.stream()
      .map(account -> {
        var portfolio = operationsService.getPortfolioSync(account.getId());
        var last100operations = operationsService.getOperationByCursorSync(
          account.getId(),
          now.minus(1, ChronoUnit.HOURS),
          now,
          null,
          100,
          null,
          null,
          false,
          true,
          false,
          null
        );
        return Tuple.of(
          account.getId(),
          Tuple.of(
            portfolio,
            last100operations
          )
        );
      })
      .collect(HashMap.collector())
    );

  }


  @RequiredArgsConstructor
  @Data
  public static class EPortfolio {
    private final Map<String, Tuple2<Portfolio, GetOperationsByCursorResponse>> portfolios;
  }


}
