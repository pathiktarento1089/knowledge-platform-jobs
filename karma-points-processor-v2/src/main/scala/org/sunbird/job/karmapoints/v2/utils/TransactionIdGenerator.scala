package org.sunbird.job.karmapoints.v2.utils

import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * Formats the `TXN-<timestamp>-<suffix>` transaction id shared by all three Karma Coin handlers
 * (POINTS_CONVERSION/COINS_REDEMPTION/COINS_REAWARD - all three insert into the same
 * `user_karma_coin_transactions` table under this id, so one generator serves all of them rather
 * than three copies of the same logic), e.g. `TXN-20260911214830-D46D82A19F73C821`.
 *
 * The timestamp makes the id human-readable/roughly time-ordered; uniqueness itself comes from
 * the UUID-derived suffix, not the timestamp - two calls in the same second (from the same JVM or
 * different Flink TaskManagers) get the same timestamp segment but effectively never the same
 * suffix. `UUID.randomUUID()` is generated locally per call with no shared/coordinated state
 * (no Cassandra table, no per-JVM counter), so it needs no cross-JVM coordination to stay safe
 * under concurrent, distributed calls - unlike a JVM-local `AtomicLong` or the timestamp alone,
 * neither of which two different TaskManagers could ever agree on.
 *
 * Called once per plan (`freezeConversionPlan`/`createAndPersistRedemptionPlan`/
 * `createAndPersistReawardPlan`) - a replay reuses the id already frozen into the persisted plan,
 * never calls this again for the same request.
 *
 * Suffix width is `config.TRANSACTION_ID_SUFFIX_LENGTH` (default 16) rather than a fixed constant,
 * so it can be widened/narrowed without a code change - clamped to 32 (a UUID has only 32 hex
 * characters once its dashes are stripped) so a misconfigured value can't throw at runtime.
 */
private[v2] object TransactionIdGenerator {

  private val TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  private val MAX_SUFFIX_LENGTH = 32

  private[v2] def generate(config: KarmaPointsV2Config): String = {
    val timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT)
    val suffixLength = math.min(config.TRANSACTION_ID_SUFFIX_LENGTH, MAX_SUFFIX_LENGTH)
    val suffix = UUID.randomUUID().toString.replace("-", "").substring(0, suffixLength).toUpperCase
    s"${config.TRANSACTION_ID_PREFIX}-$timestamp-$suffix"
  }
}
