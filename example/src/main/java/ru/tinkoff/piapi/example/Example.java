package ru.tinkoff.piapi.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.piapi.core.ApiConfig;
import ru.tinkoff.piapi.core.InvestApi;

public class Example {

  private static final Logger log = LoggerFactory.getLogger(Example.class);

  public static void main(String[] args) throws Exception {

    var api = InvestApi.create(ApiConfig.loadFromClassPath("example-bot.properties"));

    var instrumentAsync = api.runWithHeaders(
      (call, headers) -> call.getInstrumentsService()
        .findInstrument("TINKOFF")
        .whenComplete(
          (result, throwable) -> {
            log.info("headers:{} trackingId:{}", headers, headers.get("x-tracking-id"));
          }
        )
    );
    log.info("instrument:{}", instrumentAsync.get());


  }
}
