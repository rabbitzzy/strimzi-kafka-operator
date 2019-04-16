/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.test.TestUtils;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
class RollingUpdateST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    static final String NAMESPACE = "rolling-update-cluster-test";
    static final String CLUSTER_NAME = "my-cluster";
    private static final int CO_OPERATION_TIMEOUT = 60000;
    private static final int CO_OPERATION_TIMEOUT_WAIT = CO_OPERATION_TIMEOUT + 20000;
    private static final int CO_OPERATION_TIMEOUT_POLL = 2000;
    private static final String RECONCILIATION_PATTERN = "'Triggering periodic reconciliation for namespace " + NAMESPACE + "'";

    @Test
    void testRecoveryDuringZookeeperRollingUpdate() {
        // @TODO add send-recv messages during this test
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);

        String firstZkPodName = KafkaResources.zookeeperPodName(CLUSTER_NAME, 0);
        String logZkPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstZkPodName + "'";

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("Update resources for pods");

        resources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                .editZookeeper()
                .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build())
                .endZookeeper()
                .endSpec()
                .done();

        TestUtils.waitFor("Wait till rolling update of pods start", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT,
            () -> !CLIENT.pods().inNamespace(NAMESPACE).withName(firstZkPodName).isReady());

        TestUtils.waitFor("Wait till rolling update timeout", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT_WAIT,
            () -> !KUBE_CMD_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), logZkPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        String reconciliation = TimeMeasuringSystem.startOperation(Operation.NEXT_RECONCILIATION);

        LOGGER.info("Wait till another rolling update starts");
        TestUtils.waitFor("Wait till another rolling update starts", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT,
            () -> !KUBE_CMD_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, reconciliation), RECONCILIATION_PATTERN).isEmpty());

        TimeMeasuringSystem.stopOperation(reconciliation);

        // Second part
        String rollingUpdateOperation = TimeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        LOGGER.info(TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation));

        TestUtils.waitFor("Wait till rolling update timeout", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT_WAIT,
            () -> !KUBE_CMD_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation), logZkPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        TimeMeasuringSystem.stopOperation(rollingUpdateOperation);
        TimeMeasuringSystem.stopOperation(operationID);
    }

    @Test
    void testRecoveryDuringKafkaRollingUpdate() {
        // @TODO add send-recv messages during this test
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);

        String firstKafkaPodName = KafkaResources.kafkaPodName(CLUSTER_NAME, 0);
        String logKafkaPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstKafkaPodName + "'";

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("Update resources for pods");

        resources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka()
                .withResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build())
                .endKafka()
                .endSpec()
                .done();

        TestUtils.waitFor("Wait till rolling update of pods start", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT,
            () -> !CLIENT.pods().inNamespace(NAMESPACE).withName(firstKafkaPodName).isReady());

        TestUtils.waitFor("Wait till rolling update timeouted", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT_WAIT,
            () -> !KUBE_CMD_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), logKafkaPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        String reconciliation = TimeMeasuringSystem.startOperation(Operation.NEXT_RECONCILIATION);

        LOGGER.info("Wait till another rolling update starts");
        TestUtils.waitFor("Wait till another rolling update starts", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT,
            () -> !KUBE_CMD_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, reconciliation), RECONCILIATION_PATTERN).isEmpty());

        TimeMeasuringSystem.stopOperation(reconciliation);

        // Second part
        String rollingUpdateOperation = TimeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        LOGGER.info(TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation));

        TestUtils.waitFor("Wait till rolling update timedout", CO_OPERATION_TIMEOUT_POLL, CO_OPERATION_TIMEOUT_WAIT,
            () -> !KUBE_CMD_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation), logKafkaPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        TimeMeasuringSystem.stopOperation(rollingUpdateOperation);
        TimeMeasuringSystem.stopOperation(operationID);
    }

    void assertThatRollingUpdatedFinished(String rolledComponent, String stableComponent) {
        List<String> podStatuses = CLIENT.pods().inNamespace(NAMESPACE).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(rolledComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        assertThat(rolledComponent + "is fine", podStatuses.contains("Pending"));

        Map<String, Long> statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", rolledComponent, statusCount);

        assertThat("", statusCount.get("Pending"), is(1L));
        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size() - 1)));

        podStatuses = CLIENT.pods().inNamespace(NAMESPACE).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(stableComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", stableComponent, statusCount);

        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size())));
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
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE, Integer.toString(CO_OPERATION_TIMEOUT)).done();
    }

    @AfterAll
    void teardownEnvironment() {
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }
}
