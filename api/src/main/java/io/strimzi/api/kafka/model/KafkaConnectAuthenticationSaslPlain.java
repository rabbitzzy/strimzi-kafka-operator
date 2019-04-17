/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures the Kafka Connect authentication
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaConnectAuthenticationSaslPlain extends KafkaConnectAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SASL_PLAIN = "sasl-plain";

    private String username;
    private String password;

    @Description("Must be `" + TYPE_SASL_PLAIN + "`")
    @Override
    public String getType() {
        return TYPE_SASL_PLAIN;
    }

    @Description("Password used for the authentication.")
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Description("Username used for the authentication.")
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
