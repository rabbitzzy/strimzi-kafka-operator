/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
public class StrimziUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";

    private void copyModifyApply(File root) {
        Arrays.stream(root.listFiles()).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CMD_CLIENT.applyContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else if (f.getName().matches("050-Deployment.*")) {
                KUBE_CMD_CLIENT.applyContent(TestUtils.changeDeploymentNamespaceUpgrade(f, NAMESPACE));
            } else {
                KUBE_CMD_CLIENT.apply(f);
            }
        });
    }

    private void deleteInstalledYamls(File root) {
        Arrays.stream(root.listFiles()).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CMD_CLIENT.deleteContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else {
                KUBE_CMD_CLIENT.delete(f);
            }
        });
    }

    @Test
    void upgrade_0_8_2_to_HEAD() throws IOException {
        KUBE_CMD_CLIENT.namespace(NAMESPACE);
        File coDir = null;
        File kafkaEphemeralYaml = null;
        File kafkaTopicYaml = null;
        File kafkaUserYaml = null;
        try {
            // Deploy a 0.8.2 cluster operator
            // https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.8.2/strimzi-0.8.2.zip
            String url = "https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.8.2/strimzi-0.8.2.zip";
            File dir = StUtils.downloadAndUnzip(url);

            coDir = new File(dir, "strimzi-0.8.2/install/cluster-operator/");
            // Modify + apply installation files
            copyModifyApply(coDir);

            LOGGER.info("Waiting for CO deployment");
            StUtils.waitForDeploymentReady("strimzi-cluster-operator");

            // Deploy a 0.8.2. Kafka cluster
            kafkaEphemeralYaml = new File(dir, "strimzi-0.8.2/examples/kafka/kafka-ephemeral.yaml");
            KUBE_CMD_CLIENT.create(kafkaEphemeralYaml);
            // Wait for readiness
            LOGGER.info("Waiting for Zookeeper StatefulSet");
            StUtils.waitForAllStatefulSetPodsReady("my-cluster-zookeeper");
            LOGGER.info("Waiting for Kafka StatefulSet");
            StUtils.waitForAllStatefulSetPodsReady("my-cluster-kafka");
            LOGGER.info("Waiting for EO Deployment");
            StUtils.waitForDeploymentReady("my-cluster-entity-operator");

            // And a topic and a user
            kafkaTopicYaml = new File(dir, "strimzi-0.8.2/examples/topic/kafka-topic.yaml");
            KUBE_CMD_CLIENT.create(kafkaTopicYaml);
            kafkaUserYaml = new File(dir, "strimzi-0.8.2/examples/user/kafka-user.yaml");
            KUBE_CMD_CLIENT.create(kafkaUserYaml);

            String zkSsName = KafkaResources.zookeeperStatefulSetName("my-cluster");
            String kafkaSsName = KafkaResources.kafkaStatefulSetName("my-cluster");
            String eoDepName = KafkaResources.entityOperatorDeploymentName("my-cluster");
            Map<String, String> zkPods = StUtils.ssSnapshot(NAMESPACE, zkSsName);
            Map<String, String> kafkaPods = StUtils.ssSnapshot(NAMESPACE, kafkaSsName);
            Map<String, String> eoPods = StUtils.depSnapshot(NAMESPACE, eoDepName);

            List<Pod> pods = CLIENT.pods().inNamespace(NAMESPACE).withLabels(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels()).list().getItems();
            for (Pod pod : pods) {
                LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            }

            // Upgrade the CO, to current HEAD,
            LOGGER.info("Updating");
            copyModifyApply(new File("../install/cluster-operator"));
            LOGGER.info("Waiting for CO redeployment");
            StUtils.waitForDeploymentReady("strimzi-cluster-operator");

            LOGGER.info("Waiting for ZK SS roll");
            StUtils.waitTillSsHasRolled(NAMESPACE, zkSsName, zkPods);
            LOGGER.info("Checking ZK pods using new image");
            waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels(),
                    "strimzi/zookeeper:latest-kafka-2.0.0");
            LOGGER.info("Waiting for Kafka SS roll");
            StUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, kafkaPods);
            LOGGER.info("Checking Kafka pods using new image");
            waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaSsName).get().getSpec().getSelector().getMatchLabels(),
                    "strimzi/kafka:latest-kafka-2.1.0");
            LOGGER.info("Waiting for EO Dep roll");
            // Check the TO and UO also got upgraded
            StUtils.waitTillDepHasRolled(NAMESPACE, eoDepName, eoPods);
            LOGGER.info("Checking EO pod using new image");
            waitTillAllContainersUseImage(
                    CLIENT.apps().deployments().inNamespace(NAMESPACE).withName(eoDepName).get().getSpec().getSelector().getMatchLabels(),
                    0,
                    "strimzi/topic-operator:latest");
            waitTillAllContainersUseImage(
                    CLIENT.apps().deployments().inNamespace(NAMESPACE).withName(eoDepName).get().getSpec().getSelector().getMatchLabels(),
                    1,
                    "strimzi/user-operator:latest");


            // Tidy up
        } catch (KubeClusterException e) {
            if (kafkaEphemeralYaml != null) {
                KUBE_CMD_CLIENT.delete(kafkaEphemeralYaml);
            }
            if (coDir != null) {
                KUBE_CMD_CLIENT.delete(coDir);
            }
            throw e;
        } finally {
            deleteInstalledYamls(new File("../install/cluster-operator"));
        }

    }

    private void waitTillAllPodsUseImage(Map<String, String> matchLabels, String image) {
        waitTillAllContainersUseImage(matchLabels, 0, image);
    }

    private void waitTillAllContainersUseImage(Map<String, String> matchLabels, int container, String image) {
        TestUtils.waitFor("All pods matching " + matchLabels + " to have image " + image, GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT, () -> {
            List<Pod> pods1 = CLIENT.pods().inNamespace(NAMESPACE).withLabels(matchLabels).list().getItems();
            for (Pod pod : pods1) {
                if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                    return false;
                }
            }
            return true;
        });
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        createNamespace(NAMESPACE);
    }
    @AfterAll
    void teardownEnvironment() {
        deleteNamespaces();
    }


    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        deleteNamespaces();
        createNamespace(NAMESPACE);
    }
}
