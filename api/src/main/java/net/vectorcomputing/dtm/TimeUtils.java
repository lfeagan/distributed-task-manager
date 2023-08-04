package net.vectorcomputing.dtm;

import org.threeten.extra.PeriodDuration;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Objects;

import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;

public final class TimeUtils {

    private static final long NANOS_PER_SECOND_LONG = 1000000000L;

    public static Instant instantFromRFC3339(String dateTime) {
        return Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dateTime));
    }

    /**
     * Floors the timestamp to the specified origin, as though timestamps are in a finite field modulo the interval.
     * Put another way, shifts the timestamp backwards in time to the nearest (to the timestamp) interval offset by an integral multiple from the origin.
     * The minimum supported precision is milliseconds.
     * @param timestamp
     * @param origin
     * @param interval
     * @return
     */
    public static Instant alignWithInterval(final Instant timestamp, final Instant origin, final PeriodDuration interval, final ZoneId timeZone) {
        Objects.requireNonNull(timestamp, "timestamp must be specified");
        Objects.requireNonNull(origin, "origin must be specified");
        Objects.requireNonNull(interval, "interval must be specified");
        Objects.requireNonNull(timeZone, "time zone must be specified");

        // Determine if only the duration is present, so a more optimally performing method can be used
        final boolean hasPeriod = !interval.getPeriod().equals(Period.ZERO);
        final boolean hasDuration = !interval.getDuration().equals(Duration.ZERO);
        if (hasPeriod) {
            // period != 0, duration != 0
            // period != 0, duration = 0
            return _alignWithInterval(timestamp, origin, interval, timeZone);
        } else {
            if (hasDuration) {
                // period = 0, duration != 0
                return alignWithInterval(timestamp, origin, interval.getDuration());
            } else {
                // interval is all 0s (period = 0, duration = 0)
                return timestamp;
            }
        }
    }

    /**
     * Floors the timestamp to the specified origin, as though timestamps are in a finite field modulo the interval.
     * This is an internal-use method that must be used when a non-zero period is present and therefore timezones
     * and other calendar peculiarities need to be addressed.
     * @param timestamp
     * @param origin
     * @param interval
     * @param timeZone
     * @return
     */
    private static Instant _alignWithInterval(Instant timestamp, final Instant origin, final PeriodDuration interval, final ZoneId timeZone) {
        // handle trivial case
        if (origin.equals(timestamp)) {
            return timestamp;
        }

        final ZonedDateTime zonedTimestamp = ZonedDateTime.ofInstant(timestamp, timeZone);
        ZonedDateTime alignedTime = ZonedDateTime.ofInstant(origin, timeZone);
        // determine if we need to move forwards or backwards in time from the origin
        if (timestamp.isBefore(origin)) {
            while (alignedTime.isAfter(zonedTimestamp)) {
                alignedTime = alignedTime.minus(interval);
            }
        } else {
            while (alignedTime.isBefore(zonedTimestamp)) {
                alignedTime = alignedTime.plus(interval);
            }
            if (alignedTime.equals(zonedTimestamp)) {
                // do nothing
            } else {
                // move back one interval
                alignedTime = alignedTime.minus(interval);
            }
        }
        return alignedTime.toInstant();
    }

    /**
     * Aligns the specified timestamp with the nearest period interval integrally offset from the origin time.
     * Minimum supported precision is milliseconds.
     * @param timestamp
     * @param origin
     * @param interval
     * @return
     */
    public static Instant alignWithInterval(final Instant timestamp, final Instant origin, final Period interval, final ZoneId timeZone) {
        return alignWithInterval(timestamp, origin, PeriodDuration.of(interval), timeZone);
    }

    /**
     * Aligns the specified timestamp with the nearest, older duration interval integrally offset from the origin time.
     * Minimum supported precision is milliseconds.
     * Think of this as performing the floor function in a finite field of timestamps modulo the interval.
     * @param timestamp
     * @param origin
     * @param interval
     * @return
     */
    public static Instant alignWithInterval(Instant timestamp, final Instant origin, final Duration interval) {
        Objects.requireNonNull(timestamp, "timestamp must be specified");
        Objects.requireNonNull(origin, "origin must be specified");
        Objects.requireNonNull(interval, "interval must be specified");

        // handle trivial case
        if (origin.equals(timestamp)) {
            return timestamp;
        }

        long delta_t = timestamp.toEpochMilli() - origin.toEpochMilli();
        long estimated_beats = (long) (delta_t / toSeconds(interval).multiply(BigDecimal.valueOf(1000)).longValue());

        Instant alignedTime = origin;
        // determine if we need to move forwards or backwards in time from the origin
        if (timestamp.isBefore(origin)) {
            while (alignedTime.isAfter(timestamp)) {
                alignedTime = alignedTime.minus(interval);
            }
        } else {
            alignedTime = alignedTime.plus(interval.multipliedBy(estimated_beats-1));
            while (alignedTime.isBefore(timestamp)) {
                alignedTime = alignedTime.plus(interval);
            }
            if (alignedTime.equals(timestamp)) {
                return alignedTime;
            } else {
                // move back one interval
                alignedTime = alignedTime.minus(interval);
            }
        }
        return alignedTime;
    }

    /**
     * Returns the total seconds in the specified duration.
     *
     * @param interval
     * @return
     */
    public static BigDecimal toSeconds(Duration interval) {
        return BigDecimal.valueOf(interval.toNanos()).divide(BigDecimal.valueOf(NANOS_PER_SECOND_LONG));
    }
}
