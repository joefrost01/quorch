package org.neuralchilli.quorch.service;

import java.util.Optional;

/**
 * Result of loading a configuration file.
 * Provides type-safe success/failure handling with clear error messages.
 */
public sealed interface LoadResult {

    /**
     * Check if load was successful
     */
    boolean isSuccess();

    /**
     * Get the loaded item name (graph/task name)
     */
    String name();

    /**
     * Get error message if failed
     */
    Optional<String> error();

    /**
     * Successful load result
     */
    record Success(String name) implements LoadResult {
        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public Optional<String> error() {
            return Optional.empty();
        }
    }

    /**
     * Failed load result with error message
     */
    record Failure(String name, String errorMessage) implements LoadResult {
        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public Optional<String> error() {
            return Optional.of(errorMessage);
        }
    }

    /**
     * Create a success result
     */
    static LoadResult success(String name) {
        return new Success(name);
    }

    /**
     * Create a failure result
     */
    static LoadResult failure(String name, String error) {
        return new Failure(name, error);
    }

    /**
     * Create a failure result from exception
     */
    static LoadResult failure(String name, Exception e) {
        return new Failure(name, e.getMessage());
    }
}