package ru.tinkoff.piapi.example;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ExampleUtils {

  public static <K extends MessageOrBuilder> Try<Path> writeMessagesToJsonFile(List<K> messages, String rootPath, String fileName) {
    var path = Paths.get(rootPath, fileName);
    return Try.of(() -> {
        Files.writeString(
          path,
          messages.stream()
            .map(message -> Try.of(() -> JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .preservingProtoFieldNames()
                .print(message))
              .onFailure(error -> log.error("json  print", error))
            )
            .filter(Try::isSuccess)
            .map(Try::get)
            .collect(Collectors.joining("\n"))
        );
        return path;
      }
    );
  }


  interface RunnableExample extends Runnable {

  }
}
