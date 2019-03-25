/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.vertx.core.Future;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class KafkaControllerFinder {

    KafkaControllerFinder() {

    }

    Future<Integer> leader(String bootstrapBroker) {
        // TODO retry
        // TODO TLS
        Future<Integer> result = Future.future();
        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
        p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "");
        p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "");
        AdminClient ac = AdminClient.create(p);
        ac.describeCluster().controller().whenComplete((controllerNode, exception) -> {
            if (exception != null) {
                result.fail(exception);
            } else {
                result.complete(controllerNode.id());
            }
        });
        return result;
    }


}
