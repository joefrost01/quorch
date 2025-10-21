package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParameterTest {

    @Test
    void shouldCreateOptionalParameter() {
        Parameter param = Parameter.optional(ParameterType.STRING, "A description");

        assertThat(param.type()).isEqualTo(ParameterType.STRING);
        assertThat(param.required()).isFalse();
        assertThat(param.defaultValue()).isNull();
        assertThat(param.description()).isEqualTo("A description");
    }

    @Test
    void shouldCreateRequiredParameter() {
        Parameter param = Parameter.required(ParameterType.INTEGER, "Required param");

        assertThat(param.type()).isEqualTo(ParameterType.INTEGER);
        assertThat(param.required()).isTrue();
        assertThat(param.defaultValue()).isNull();
    }

    @Test
    void shouldCreateParameterWithDefault() {
        Parameter param = Parameter.withDefault(ParameterType.STRING, "default-value", "With default");

        assertThat(param.type()).isEqualTo(ParameterType.STRING);
        assertThat(param.required()).isFalse();
        assertThat(param.defaultValue()).isEqualTo("default-value");
    }

    @Test
    void shouldRejectNullType() {
        assertThatThrownBy(() -> new Parameter(null, null, false, "desc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Parameter type cannot be null");
    }
}