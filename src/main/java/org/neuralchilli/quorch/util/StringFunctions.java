package org.neuralchilli.quorch.util;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Built-in string functions for JEXL expressions.
 * Provides string manipulation and generation capabilities.
 */
public class StringFunctions {

    private static final Pattern SLUG_PATTERN = Pattern.compile("[^a-z0-9-]+");

    /**
     * Generate a random UUID
     */
    public String uuid() {
        return UUID.randomUUID().toString();
    }

    /**
     * Convert a string to a URL-safe slug
     * Example: "My Workflow Name" -> "my-workflow-name"
     */
    public String slugify(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }

        String slug = input.toLowerCase()
                .trim()
                .replaceAll("\\s+", "-");  // Replace whitespace with hyphens

        slug = SLUG_PATTERN.matcher(slug).replaceAll("");  // Remove invalid chars

        // Remove duplicate hyphens
        slug = slug.replaceAll("-+", "-");

        // Remove leading/trailing hyphens
        slug = slug.replaceAll("^-|-$", "");

        return slug;
    }

    /**
     * Join strings with a separator
     */
    public String join(String separator, Object... parts) {
        if (parts == null || parts.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(parts[i] != null ? parts[i].toString() : "");
        }
        return sb.toString();
    }

    /**
     * Pad string to the left
     */
    public String padLeft(String input, int length, char padChar) {
        if (input == null) {
            input = "";
        }
        if (input.length() >= length) {
            return input;
        }

        StringBuilder sb = new StringBuilder();
        int padCount = length - input.length();
        for (int i = 0; i < padCount; i++) {
            sb.append(padChar);
        }
        sb.append(input);
        return sb.toString();
    }

    /**
     * Pad string to the right
     */
    public String padRight(String input, int length, char padChar) {
        if (input == null) {
            input = "";
        }
        if (input.length() >= length) {
            return input;
        }

        StringBuilder sb = new StringBuilder(input);
        int padCount = length - input.length();
        for (int i = 0; i < padCount; i++) {
            sb.append(padChar);
        }
        return sb.toString();
    }

    /**
     * Convert to snake_case
     */
    public String toSnakeCase(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }

        // Insert underscore before uppercase letters
        String result = input.replaceAll("([a-z])([A-Z])", "$1_$2");
        // Replace spaces and hyphens with underscores
        result = result.replaceAll("[\\s-]+", "_");
        // Convert to lowercase
        return result.toLowerCase();
    }

    /**
     * Convert to camelCase
     */
    public String toCamelCase(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }

        String[] parts = input.split("[\\s_-]+");
        StringBuilder result = new StringBuilder(parts[0].toLowerCase());

        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            if (!part.isEmpty()) {
                result.append(Character.toUpperCase(part.charAt(0)));
                result.append(part.substring(1).toLowerCase());
            }
        }

        return result.toString();
    }
}