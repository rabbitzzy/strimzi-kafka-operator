/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);

    static final String NAMESPACE = "helm-chart-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_NAME = "test-topic";

    @Test
    void testDeployKafkaClusterViaHelmChart() {
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        resources().topic(CLUSTER_NAME, TOPIC_NAME).done();
        StUtils.waitForAllStatefulSetPodsReady(zookeeperClusterName(CLUSTER_NAME));
        StUtils.waitForAllStatefulSetPodsReady(kafkaClusterName(CLUSTER_NAME));
    }

    @BeforeEach
    void createTestResources() {
        createResources();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        deleteResources();
        waitForDeletion(TEARDOWN_GLOBAL_WAIT, NAMESPACE);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        createNamespace(NAMESPACE);
        deployClusterOperatorViaHelmChart();
    }

    @AfterAll
    void teardownEnvironment() {
        deleteClusterOperatorViaHelmChart();
        deleteNamespaces();
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        deleteClusterOperatorViaHelmChart();
        deleteNamespaces();
        createNamespace(NAMESPACE);
    }
}
