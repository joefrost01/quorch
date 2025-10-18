package org.neuralchilli.quorch.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class TaskNodeTest {

    @Test
    void shouldCreateRegularTaskNode() {
        TaskNode node = TaskNode.regular("my-task");

        assertThat(node.taskName()).isEqualTo("my-task");
        assertThat(node.isGlobal()).isFalse();
        assertThat(node.globalTaskName()).isNull();
        assertThat(node.getEffectiveName()).isEqualTo("my-task");
        assertThat(node.getGlobalTaskName()).isNull();
    }

    @Test
    void shouldCreateGlobalTaskNode() {
        TaskNode node = TaskNode.global("load-data", "load-market-data");

        assertThat(node.taskName()).isEqualTo("load-data");
        assertThat(node.isGlobal()).isTrue();
        assertThat(node.globalTaskName()).isEqualTo("load-market-data");
        assertThat(node.getEffectiveName()).isEqualTo("load-data");
        assertThat(node.getGlobalTaskName()).isEqualTo("load-market-data");
    }

    @Test
    void shouldEqualityBeBasedOnTaskName() {
        TaskNode node1 = new TaskNode("task-a", false, null);
        TaskNode node2 = new TaskNode("task-a", true, "global-task");
        TaskNode node3 = new TaskNode("task-b", false, null);

        // Same task name = equal (for graph operations)
        assertThat(node1).isEqualTo(node2);
        assertThat(node1.hashCode()).isEqualTo(node2.hashCode());

        // Different task name = not equal
        assertThat(node1).isNotEqualTo(node3);
        assertThat(node1.hashCode()).isNotEqualTo(node3.hashCode());
    }

    @Test
    void shouldRejectNullTaskName() {
        assertThatThrownBy(() -> new TaskNode(null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task name cannot be null");

        assertThatThrownBy(() -> new TaskNode("", false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task name cannot be null");
    }

    @Test
    void shouldHaveReadableToString() {
        TaskNode regular = TaskNode.regular("my-task");
        assertThat(regular.toString()).contains("my-task");
        assertThat(regular.toString()).doesNotContain("global");

        TaskNode global = TaskNode.global("load-data", "load-market-data");
        assertThat(global.toString()).contains("load-data");
        assertThat(global.toString()).contains("global");
        assertThat(global.toString()).contains("load-market-data");
    }

    @Test
    void shouldBeSerializable() {
        // Record types are serializable if all their components are
        TaskNode node = TaskNode.regular("test");

        // This test just verifies the type implements Serializable
        assertThat(node).isInstanceOf(java.io.Serializable.class);
    }
}