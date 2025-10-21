package org.neuralchilli.quorch.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class CycleDetectedExceptionTest {

    @Test
    void shouldCreateWithMessage() {
        CycleDetectedException exception = new CycleDetectedException("Cycle detected");

        assertThat(exception).isInstanceOf(RuntimeException.class);
        assertThat(exception.getMessage()).isEqualTo("Cycle detected");
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void shouldCreateWithMessageAndCause() {
        Throwable cause = new IllegalStateException("Root cause");
        CycleDetectedException exception = new CycleDetectedException(
                "Cycle detected",
                cause
        );

        assertThat(exception.getMessage()).isEqualTo("Cycle detected");
        assertThat(exception.getCause()).isEqualTo(cause);
    }

    @Test
    void shouldBeThrowable() {
        assertThatThrownBy(() -> {
            throw new CycleDetectedException("Test cycle");
        })
                .isInstanceOf(CycleDetectedException.class)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Test cycle");
    }

    @Test
    void shouldPreserveStackTrace() {
        try {
            methodThatThrowsCycle();
            fail("Should have thrown exception");
        } catch (CycleDetectedException e) {
            assertThat(e.getStackTrace()).isNotEmpty();
            assertThat(e.getStackTrace()[0].getMethodName())
                    .isEqualTo("methodThatThrowsCycle");
        }
    }

    private void methodThatThrowsCycle() {
        throw new CycleDetectedException("Cycle in graph");
    }
}