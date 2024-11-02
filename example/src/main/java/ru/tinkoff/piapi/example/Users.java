package ru.tinkoff.piapi.example;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.tinkoff.piapi.contract.v1.Account;
import ru.tinkoff.piapi.contract.v1.GetMarginAttributesResponse;
import ru.tinkoff.piapi.contract.v1.GetUserTariffResponse;
import ru.tinkoff.piapi.core.UsersService;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class Users {

  private final UsersService usersService;


  public UserInfo getUserInfo() {
    var accounts = usersService.getAccountsSync();
    var tariff = usersService.getUserTariffSync();
    var accountsWithMarginAttrs = accounts.stream()
      .map(account -> {
        log.debug("Account: {}", account);
        try {
          var accountMarginAttrs = usersService.getMarginAttributesSync(account.getId());
          return Tuple.of(account, accountMarginAttrs);
        } catch (Exception error) {
          log.error("error load margin attributes for account: {}", account);
          return Tuple.of(account, GetMarginAttributesResponse.getDefaultInstance());
        }

      })
      .collect(Collectors.toList());
    ;
    return new UserInfo(accountsWithMarginAttrs, tariff);
  }

  @RequiredArgsConstructor
  @Data
  public static class UserInfo {
    private final List<Tuple2<Account, GetMarginAttributesResponse>> accountsWithAttributes;
    private final GetUserTariffResponse tariff;

    public List<Account> getAccounts() {
      return accountsWithAttributes
        .stream()
        .map(Tuple2::_1)
        .collect(Collectors.toList());
    }
  }
}
