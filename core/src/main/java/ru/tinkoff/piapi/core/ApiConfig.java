package ru.tinkoff.piapi.core;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.Properties;

@RequiredArgsConstructor
@Getter
@Slf4j
@With
public class ApiConfig {
  private final static String DEFAULT_APP_NM = "t-invest-api-java-sdk";
  private final static String DEFAULT_CONFIG_PATH = "config.properties";
  private final static long DEFAULT_CONNECTION_TIMEOUT_MS = 5000;
  private final static long DEFAULT_REQUEST_TIMEOUT_MS = 60000;

  private final long connectionTimeoutMs;
  private final long requestTimeoutMs;
  private final String appNm;
  private final String sandboxTarget;
  private final String sandboxToken;
  private final String target;
  private final String token;
  private final int keepAliveSec;
  private final int keepAliveTimeoutSec;

  public static ApiConfig defaultConfig() {
    var props = loadInternalProps(DEFAULT_CONFIG_PATH);
    var defaultConfig = loadFromProps(props);
    return Optional.of(defaultConfig)
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_TARGET_SANDBOX"))
        .map(cfg::withSandboxTarget)
        .or(() -> Optional.of(cfg))
      )
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_CONNECTION_TIMEOUT"))
        .map(availableTimeOutValue -> {
          try {
            return Duration.parse(availableTimeOutValue);
          } catch (DateTimeParseException e) {
            log.error("error parse connectionTimeout config reset to default value", e);
            return Duration.ofMillis(DEFAULT_CONNECTION_TIMEOUT_MS);
          }
        })
        .map(Duration::toMillis)
        .map(cfg::withConnectionTimeoutMs)
        .or(() -> Optional.of(cfg))
      )
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_REQUEST_TIMEOUT"))
        .map(availableTimeOutValue -> {
          try {
            return Duration.parse(availableTimeOutValue);
          } catch (DateTimeParseException e) {
            log.error("error parse requestTimeout config reset to default value", e);
            return Duration.ofMillis(DEFAULT_REQUEST_TIMEOUT_MS);
          }
        })
        .map(Duration::toMillis)
        .map(cfg::withConnectionTimeoutMs)
        .or(() -> Optional.of(cfg))
      )
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_TARGET"))
        .map(cfg::withTarget)
        .or(() -> Optional.of(cfg))
      )
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_TOKEN"))
        .map(cfg::withToken)
        .or(() -> Optional.of(cfg))
      )
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_TOKEN_SANDBOX"))
        .map(cfg::withSandboxToken)
        .or(() -> Optional.of(cfg))
      )
      .flatMap(cfg -> Optional.ofNullable(System.getenv("TINKOFF_INVEST_API_APP_NM"))
        .map(cfg::withAppNm)
        .or(() -> Optional.of(cfg))
      ).orElseThrow(() -> new RuntimeException("can't load default config"));
  }

  public static ApiConfig loadFromProps(Properties props) {
    var sandboxTarget = props.getProperty("ru.tinkoff.piapi.core.sandbox.target");
    Duration connectionTimeout;
    try {
      var availableTimeOutValue = props.getProperty("ru.tinkoff.piapi.core.connection-timeout");
      connectionTimeout = Duration.parse(availableTimeOutValue);
    } catch (DateTimeParseException e) {
      connectionTimeout = Duration.ofMillis(DEFAULT_CONNECTION_TIMEOUT_MS);
      log.error("error parse connectionTimeout config reset to default value", e);
    }
    Duration requestTimeout;
    try {
      var availableTimeOutValue = props.getProperty("ru.tinkoff.piapi.core.request-timeout");
      requestTimeout = Duration.parse(availableTimeOutValue);
    } catch (DateTimeParseException e) {
      requestTimeout = Duration.ofMillis(DEFAULT_REQUEST_TIMEOUT_MS);
      log.error("error parse requestTimeout config reset to default value", e);
    }
    var target = props.getProperty("ru.tinkoff.piapi.core.api.target");
    var sandboxToken = props.getProperty("ru.tinkoff.piapi.core.api.sandbox-token");
    var token = props.getProperty("ru.tinkoff.piapi.core.api.token");
    ;
    return new ApiConfig(
      connectionTimeout.toMillis(),
      requestTimeout.toMillis(),
      DEFAULT_APP_NM,
      sandboxTarget,
      sandboxToken,
      target,
      token,
      60,
      60
    );
  }

  public static ApiConfig loadFromClassPath(String configResourceName) {
    return loadFromProps(loadInternalProps(configResourceName));
  }

  private static Properties loadInternalProps(String configResourceName) {
    var loader = Thread.currentThread().getContextClassLoader();
    var props = new Properties();
    try (var resourceStream = loader.getResourceAsStream(configResourceName)) {
      props.load(resourceStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return props;
  }
}
