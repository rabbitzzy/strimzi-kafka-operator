/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures the TCP listener of Kafka broker
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaListenerPlain implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    private KafkaListenerAuthentication authentication;
    private List<NetworkPolicyPeer> networkPolicyPeers;

    @Description("Authentication configuration for this listener. " +
            "Since this listener does not use TLS transport you cannot configure an authentication with `type: tls`.")
    public KafkaListenerAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaListenerAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("List of peers which should be able to connect to this listener. " +
            "Peers in this list are combined using a logical OR operation. " +
            "If this field is empty or missing, all connections will be allowed for this listener. " +
            "If this field is present and contains at least one item, the listener only allows the traffic which matches at least one item in this list.")
    @KubeLink(group = "networking", version = "v1", kind = "networkpolicypeer")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<NetworkPolicyPeer> getNetworkPolicyPeers() {
        return networkPolicyPeers;
    }

    public void setNetworkPolicyPeers(List<NetworkPolicyPeer> networkPolicyPeers) {
        this.networkPolicyPeers = networkPolicyPeers;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
