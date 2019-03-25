/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.debezium.kafka.KafkaCluster;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;

@RunWith(VertxUnitRunner.class)
public class KafkaControllerFinderTest {

    private KafkaCluster kafkaCluster;

    @Before
    public void setUp() throws IOException {
        kafkaCluster = new KafkaCluster()
            .addBrokers(1)
            .deleteDataPriorToStartup(true)
            .deleteDataUponShutdown(true)
            .usingDirectory(Files.createTempDirectory("operator-integration-test").toFile())
            .startup();
    }

    @After
    public void tearDown() {
        kafkaCluster.shutdown();
    }

    @Test
    public void test(TestContext context) {
        Async async = context.async();
        new KafkaControllerFinder().leader(kafkaCluster.brokerList()).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            } else {
                context.assertEquals(1, ar.result());
            }
            async.complete();
        });
    }
}
