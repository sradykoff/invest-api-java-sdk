package ru.tinkoff.piapi.example.domain.portfolio;

import io.vavr.control.Either;
import lombok.Getter;
import ru.tinkoff.piapi.core.models.Quantity;


public class BlockedAmount {
  static BlockedAmount ZERO = new BlockedAmount(Quantity.ZERO, Quantity.ZERO);
  static BlockedAmount ONE = new BlockedAmount(Quantity.ONE, Quantity.ZERO);

  private final Quantity total;
  @Getter
  private final Quantity blocked;

  BlockedAmount(Quantity total, Quantity blocked) {
    this.total = total;
    this.blocked = blocked;
  }

  public Quantity available() {
    if (blocked.isEquals(Quantity.ZERO)) return total;
    return total.subtract(blocked);
  }

  public BlockedAmount add(Quantity total) {
    if (blocked.isEquals(Quantity.ZERO))
      return new BlockedAmount(this.total.add(total), Quantity.ZERO);
    else {
      var differenceBlocked = this.blocked.subtract(total);
      if (differenceBlocked.isLess(Quantity.ZERO)) {
        return new BlockedAmount(this.total.add(differenceBlocked.abs()), Quantity.ZERO);
      } else if (differenceBlocked.isEquals(Quantity.ZERO)) {
        return new BlockedAmount(this.total, Quantity.ZERO);
      } else {
        return new BlockedAmount(this.total.add(differenceBlocked), Quantity.ZERO);
      }
    }
  }


  public Either<NoBalanceAvailable, BlockedAmount> block(Quantity blocked) {
    if (blocked.isLessOrEquals(available())) {
      return Either.right(new BlockedAmount(this.total, this.blocked.add(blocked)));
    } else
      return Either.left(new NoBalanceAvailable("Not enough balance"));
  }

  public Either<NoBalanceAvailable, BlockedAmount> unblock(Quantity unblocked) {
    if (unblocked.isLessOrEquals(blocked)) {
      return Either.right(new BlockedAmount(this.total, this.blocked.subtract(unblocked)));
    } else
      return Either.left(new NoBalanceAvailable("Not enough blocked"));
  }

  public Either<NoBalanceAvailable, BlockedAmount> withdrawBlocked(Quantity quantity) {
    if (quantity.isLessOrEquals(blocked)) {
      return Either.right(new BlockedAmount(this.total, this.blocked.subtract(quantity)));
    } else
      return Either.left(new NoBalanceAvailable("Not enough blocked"));
  }

  public static BlockedAmount zero() {
    return ZERO;
  }

  public static BlockedAmount one() {
    return ONE;
  }

  public static BlockedAmount of(Quantity total) {
    return new BlockedAmount(total, Quantity.ZERO);
  }

}
