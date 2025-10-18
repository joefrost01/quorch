package org.neuralchilli.quorch.core;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Built-in date functions for JEXL expressions.
 * Provides date manipulation and formatting capabilities.
 */
public class DateFunctions {

    /**
     * Get today's date in ISO format (yyyy-MM-dd)
     */
    public String today() {
        return LocalDate.now().toString();
    }

    /**
     * Get current date/time in specified format
     * @param pattern DateTimeFormatter pattern
     */
    public String now(String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDate.now().format(formatter);
    }

    /**
     * Add time to a date
     * @param dateStr Date string in ISO format
     * @param amount Amount to add (can be negative)
     * @param unit Unit: "days", "weeks", "months", "years"
     */
    public String add(String dateStr, long amount, String unit) {
        LocalDate date = LocalDate.parse(dateStr);

        return switch (unit.toLowerCase()) {
            case "days", "day" -> date.plusDays(amount).toString();
            case "weeks", "week" -> date.plusWeeks(amount).toString();
            case "months", "month" -> date.plusMonths(amount).toString();
            case "years", "year" -> date.plusYears(amount).toString();
            default -> throw new IllegalArgumentException(
                    "Invalid unit: " + unit + ". Use: days, weeks, months, years"
            );
        };
    }

    /**
     * Subtract time from a date
     * @param dateStr Date string in ISO format
     * @param amount Amount to subtract
     * @param unit Unit: "days", "weeks", "months", "years"
     */
    public String sub(String dateStr, long amount, String unit) {
        return add(dateStr, -amount, unit);
    }

    /**
     * Format a date string
     * @param dateStr Date string in ISO format
     * @param pattern Target format pattern
     */
    public String format(String dateStr, String pattern) {
        LocalDate date = LocalDate.parse(dateStr);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return date.format(formatter);
    }

    /**
     * Get days between two dates
     * @param startDate Start date in ISO format
     * @param endDate End date in ISO format
     */
    public long daysBetween(String startDate, String endDate) {
        LocalDate start = LocalDate.parse(startDate);
        LocalDate end = LocalDate.parse(endDate);
        return ChronoUnit.DAYS.between(start, end);
    }

    /**
     * Check if a date is before another
     */
    public boolean isBefore(String dateStr, String otherDateStr) {
        LocalDate date = LocalDate.parse(dateStr);
        LocalDate other = LocalDate.parse(otherDateStr);
        return date.isBefore(other);
    }

    /**
     * Check if a date is after another
     */
    public boolean isAfter(String dateStr, String otherDateStr) {
        LocalDate date = LocalDate.parse(dateStr);
        LocalDate other = LocalDate.parse(otherDateStr);
        return date.isAfter(other);
    }
}