package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskTest {

    @Test
    void shouldCreateSimpleTask() {
        Task task = Task.builder("my-task")
                .command("echo")
                .args(List.of("hello"))
                .build();

        assertThat(task.name()).isEqualTo("my-task");
        assertThat(task.command()).isEqualTo("echo");
        assertThat(task.args()).containsExactly("hello");
        assertThat(task.global()).isFalse();
        assertThat(task.timeout()).isEqualTo(3600);
        assertThat(task.retry()).isEqualTo(3);
    }

    @Test
    void shouldCreateGlobalTask() {
        Task task = Task.builder("load-data")
                .global(true)
                .key("load_${params.date}")
                .params(Map.of(
                        "date", Parameter.required(ParameterType.STRING, "Date to load")
                ))
                .command("python")
                .args(List.of("load.py"))
                .build();

        assertThat(task.global()).isTrue();
        assertThat(task.key()).isEqualTo("load_${params.date}");
        assertThat(task.params()).hasSize(1);
    }

    @Test
    void shouldRejectInvalidName() {
        assertThatThrownBy(() ->
                Task.builder("Invalid_Name")
                        .command("echo")
                        .build()
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must match pattern");
    }

    @Test
    void shouldRejectGlobalTaskWithoutKey() {
        assertThatThrownBy(() ->
                Task.builder("task")
                        .global(true)
                        .command("echo")
                        .build()
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Global tasks must have a key");
    }

    @Test
    void shouldRejectGlobalTaskWithoutParams() {
        assertThatThrownBy(() ->
                Task.builder("task")
                        .global(true)
                        .key("key_${params.x}")
                        .command("echo")
                        .build()
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Global tasks must define parameters");
    }
}