package org.sunbird.job.karmapoints.v2.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.v2.config.KarmaPointsV2Config
import org.sunbird.job.util.JSONUtil

import java.util.UUID

/**
 * Publishes an EXT_COURSE_ENROLLMENT event to `user.paid.course.enrolment` once a COINS_REDEMPTION
 * DEBIT is fully persisted (wallet updated, transaction written, lookup SUCCESS) - same
 * standalone-KafkaProducer pattern as [[FailedEventProducer]], constructed once per subtask in
 * `open()` rather than reusing the consumer's client.
 */
class PaidCourseEnrolmentProducer(config: KarmaPointsV2Config) extends Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[PaidCourseEnrolmentProducer])
  @transient private var producer: KafkaProducer[String, String] = _

  def init(): Unit = {
    val props = config.kafkaProducerProperties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producer = new KafkaProducer[String, String](props)
  }

  def send(userId: String, contextType: String, contextId: String, coinsRedeemed: Long,
           transactionId: String, createdAt: Long): Unit = {
    try {
      val data = new java.util.HashMap[String, Any]()
      data.put("reqId", UUID.randomUUID().toString)
      data.put("eid", config.EVENT_TYPE_EXT_COURSE_ENROLLMENT)
      data.put("ets", System.currentTimeMillis())
      data.put("userId", userId)
      data.put("operation", config.OPERATION_ENROLLMENT)
      data.put("actionType", config.ACTION_TYPE_ENROLLMENT)
      data.put("coinsRedeemed", coinsRedeemed)
      data.put("contextType", contextType)
      data.put("contextId", contextId)
      data.put("transactionId", transactionId)
      data.put("createdAt", createdAt)

      val eventPayload = new java.util.HashMap[String, Any]()
      eventPayload.put("eventType", config.EVENT_TYPE_EXT_COURSE_ENROLLMENT)
      eventPayload.put("data", data)

      val json = JSONUtil.serialize(eventPayload)
      val key = if (userId != null && userId.nonEmpty) userId else "unknown"
      producer.send(new ProducerRecord[String, String](config.kafkaPaidCourseEnrolmentTopic, key, json))
    } catch {
      case ex: Exception =>
        // Best-effort, same as the Redis wallet-cache refresh this follows: the redemption is
        // already committed by this point, so a publish failure must not fail/retry the event.
        logger.error(s"Failed to publish paid-course-enrolment event to ${config.kafkaPaidCourseEnrolmentTopic} " +
          s"for userId=$userId, transactionId=$transactionId", ex)
    }
  }

  def close(): Unit = {
    try {
      if (producer != null) producer.close()
    } catch {
      case ex: Exception => logger.warn("Error closing PaidCourseEnrolmentProducer", ex)
    }
  }
}
