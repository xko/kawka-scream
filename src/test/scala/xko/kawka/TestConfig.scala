package xko.kawka

import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.test.TestUtils

import java.util.Properties
import scala.jdk.CollectionConverters._

trait TestConfig {
    val config: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-integration-test")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
        p
    }

}
