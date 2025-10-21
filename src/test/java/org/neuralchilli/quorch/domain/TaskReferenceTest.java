package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for TaskReference domain object.
 * Verifies construction, validation, and factory methods.
 */
class TaskReferenceTest {

    @Test
    void shouldCreateGlobalTaskReference() {
        // Given: Global task reference data
        Map<String, Object> params = Map.of("date", "2025-10-17", "region", "us");
        List<String> dependsOn = List.of("task-a", "task-b");

        // When: Create global reference
        TaskReference ref = TaskReference.toGlobal("load-market-data", params, dependsOn);

        // Then: Should be configured correctly
        assertThat(ref.isGlobalReference()).isTrue();
        assertThat(ref.isInline()).isFalse();
        assertThat(ref.taskName()).isEqualTo("load-market-data");
        assertThat(ref.inlineTask()).isNull();
        assertThat(ref.params()).containsEntry("date", "2025-10-17");
        assertThat(ref.params()).containsEntry("region", "us");
        assertThat(ref.dependsOn()).containsExactly("task-a", "task-b");
        assertThat(ref.getEffectiveName()).isEqualTo("load-market-data");
    }

    @Test
    void shouldCreateGlobalTaskReferenceWithoutParams() {
        // When: Create global reference with no params
        TaskReference ref = TaskReference.toGlobal("simple-task", null, null);

        // Then: Should handle null params and dependsOn
        assertThat(ref.params()).isEmpty();
        assertThat(ref.dependsOn()).isEmpty();
    }

    @Test
    void shouldCreateGlobalTaskReferenceWithEmptyCollections() {
        // When: Create global reference with empty collections
        TaskReference ref = TaskReference.toGlobal("simple-task", Map.of(), List.of());

        // Then: Should handle empty collections
        assertThat(ref.params()).isEmpty();
        assertThat(ref.dependsOn()).isEmpty();
    }

    @Test
    void shouldCreateInlineTaskReference() {
        // Given: Inline task
        Task task = new Task(
                "inline-task",
                false,
                null,
                Map.of(),
                "echo",
                List.of("hello"),
                Map.of("ENV_VAR", "value"),
                300,
                2,
                Collections.EMPTY_LIST
        );
        List<String> dependsOn = List.of("upstream-task");

        // When: Create inline reference
        TaskReference ref = TaskReference.inline(task, dependsOn);

        // Then: Should be configured correctly
        assertThat(ref.isInline()).isTrue();
        assertThat(ref.isGlobalReference()).isFalse();
        assertThat(ref.taskName()).isNull();
        assertThat(ref.inlineTask()).isEqualTo(task);
        assertThat(ref.params()).isEmpty(); // Inline tasks don't have param overrides
        assertThat(ref.dependsOn()).containsExactly("upstream-task");
        assertThat(ref.getEffectiveName()).isEqualTo("inline-task");
    }

    @Test
    void shouldCreateInlineTaskReferenceWithoutDependencies() {
        // Given: Inline task
        Task task = new Task(
                "no-deps-task",
                false,
                null,
                Map.of(),
                "python",
                List.of("script.py"),
                Map.of(),
                600,
                3,
                Collections.EMPTY_LIST
        );

        // When: Create inline reference with no dependencies
        TaskReference ref = TaskReference.inline(task, null);

        // Then: Should handle null dependencies
        assertThat(ref.dependsOn()).isEmpty();
    }

    @Test
    void shouldRejectNullTaskNameAndInlineTask() {
        // When/Then: Creating reference with both null should fail
        assertThatThrownBy(() -> new TaskReference(null, null, Map.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must have either taskName (global) or inlineTask (inline)");
    }

    @Test
    void shouldRejectBothTaskNameAndInlineTask() {
        // Given: Both taskName and inlineTask
        Task task = new Task(
                "task",
                false,
                null,
                Map.of(),
                "echo",
                List.of("hello"),
                Map.of(),
                300,
                2,
                Collections.EMPTY_LIST
        );

        // When/Then: Creating reference with both should fail
        assertThatThrownBy(() -> new TaskReference("global-task", task, Map.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot have both taskName and inlineTask");
    }

    @Test
    void shouldCreateImmutableParams() {
        // Given: Mutable map
        Map<String, Object> mutableParams = new java.util.HashMap<>();
        mutableParams.put("key", "value");

        // When: Create reference
        TaskReference ref = TaskReference.toGlobal("task", mutableParams, null);

        // Then: Modifying original should not affect reference
        mutableParams.put("new_key", "new_value");
        assertThat(ref.params()).hasSize(1);
        assertThat(ref.params()).doesNotContainKey("new_key");

        // And: Returned params should be unmodifiable
        assertThatThrownBy(() -> ref.params().put("another", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldCreateImmutableDependencies() {
        // Given: Mutable list
        List<String> mutableDeps = new java.util.ArrayList<>();
        mutableDeps.add("dep1");

        // When: Create reference
        TaskReference ref = TaskReference.toGlobal("task", null, mutableDeps);

        // Then: Modifying original should not affect reference
        mutableDeps.add("dep2");
        assertThat(ref.dependsOn()).hasSize(1);
        assertThat(ref.dependsOn()).doesNotContain("dep2");

        // And: Returned list should be unmodifiable
        assertThatThrownBy(() -> ref.dependsOn().add("dep3"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        // Given: Two identical references
        TaskReference ref1 = TaskReference.toGlobal(
                "task",
                Map.of("param", "value"),
                List.of("dep")
        );
        TaskReference ref2 = TaskReference.toGlobal(
                "task",
                Map.of("param", "value"),
                List.of("dep")
        );

        // Then: Should be equal
        assertThat(ref1).isEqualTo(ref2);
        assertThat(ref1.hashCode()).isEqualTo(ref2.hashCode());
    }

    @Test
    void shouldNotEqualDifferentTaskName() {
        // Given: References with different task names
        TaskReference ref1 = TaskReference.toGlobal("task-a", null, null);
        TaskReference ref2 = TaskReference.toGlobal("task-b", null, null);

        // Then: Should not be equal
        assertThat(ref1).isNotEqualTo(ref2);
    }

    @Test
    void shouldNotEqualDifferentParams() {
        // Given: References with different params
        TaskReference ref1 = TaskReference.toGlobal("task", Map.of("a", "1"), null);
        TaskReference ref2 = TaskReference.toGlobal("task", Map.of("a", "2"), null);

        // Then: Should not be equal
        assertThat(ref1).isNotEqualTo(ref2);
    }

    @Test
    void shouldNotEqualDifferentDependencies() {
        // Given: References with different dependencies
        TaskReference ref1 = TaskReference.toGlobal("task", null, List.of("dep1"));
        TaskReference ref2 = TaskReference.toGlobal("task", null, List.of("dep2"));

        // Then: Should not be equal
        assertThat(ref1).isNotEqualTo(ref2);
    }

    @Test
    void shouldEqualInlineTaskReferences() {
        // Given: Two identical inline task references
        Task task1 = new Task(
                "task",
                false,
                null,
                Map.of(),
                "echo",
                List.of("hello"),
                Map.of(),
                300,
                2,
                Collections.EMPTY_LIST
        );
        Task task2 = new Task(
                "task",
                false,
                null,
                Map.of(),
                "echo",
                List.of("hello"),
                Map.of(),
                300,
                2,
                Collections.EMPTY_LIST
        );

        TaskReference ref1 = TaskReference.inline(task1, List.of("dep"));
        TaskReference ref2 = TaskReference.inline(task2, List.of("dep"));

        // Then: Should be equal (assuming Task implements equals correctly)
        assertThat(ref1).isEqualTo(ref2);
    }

    @Test
    void shouldNotEqualGlobalAndInlineReferences() {
        // Given: Global and inline references with same name
        Task task = new Task(
                "task",
                false,
                null,
                Map.of(),
                "echo",
                List.of("hello"),
                Map.of(),
                300,
                2,
                Collections.EMPTY_LIST
        );

        TaskReference global = TaskReference.toGlobal("task", null, null);
        TaskReference inline = TaskReference.inline(task, null);

        // Then: Should not be equal
        assertThat(global).isNotEqualTo(inline);
    }

    @Test
    void shouldNotEqualNull() {
        // Given: A reference
        TaskReference ref = TaskReference.toGlobal("task", null, null);

        // Then: Should not equal null
        assertThat(ref).isNotEqualTo(null);
    }

    @Test
    void shouldNotEqualDifferentClass() {
        // Given: A reference
        TaskReference ref = TaskReference.toGlobal("task", null, null);

        // Then: Should not equal different class
        assertThat(ref).isNotEqualTo("not a TaskReference");
    }

    @Test
    void shouldEqualItself() {
        // Given: A reference
        TaskReference ref = TaskReference.toGlobal("task", null, null);

        // Then: Should equal itself
        assertThat(ref).isEqualTo(ref);
    }

    @Test
    void shouldHaveConsistentHashCode() {
        // Given: A reference
        TaskReference ref = TaskReference.toGlobal(
                "task",
                Map.of("param", "value"),
                List.of("dep")
        );

        // When: Get hash code multiple times
        int hash1 = ref.hashCode();
        int hash2 = ref.hashCode();

        // Then: Should be consistent
        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void shouldHaveReadableToString() {
        // Given: A global reference
        TaskReference ref = TaskReference.toGlobal(
                "load-data",
                Map.of("date", "2025-10-17"),
                List.of("extract")
        );

        // When: Get string representation
        String str = ref.toString();

        // Then: Should contain key information
        assertThat(str).contains("TaskReference");
        assertThat(str).contains("taskName=load-data");
        assertThat(str).contains("date");
        assertThat(str).contains("extract");
    }

    @Test
    void shouldHandleComplexParamTypes() {
        // Given: Reference with complex parameter types
        Map<String, Object> complexParams = Map.of(
                "string", "value",
                "number", 42,
                "boolean", true,
                "nested", Map.of("key", "value")
        );

        // When: Create reference
        TaskReference ref = TaskReference.toGlobal("task", complexParams, null);

        // Then: All param types should be preserved
        assertThat(ref.params()).containsEntry("string", "value");
        assertThat(ref.params()).containsEntry("number", 42);
        assertThat(ref.params()).containsEntry("boolean", true);
        assertThat(ref.params()).containsKey("nested");
    }

    @Test
    void shouldBeSerializable() {
        // Given: A task reference
        TaskReference ref = TaskReference.toGlobal(
                "task",
                Map.of("param", "value"),
                List.of("dep")
        );

        // Then: Should be serializable (has serialVersionUID)
        assertThat(ref).isInstanceOf(java.io.Serializable.class);
    }
}