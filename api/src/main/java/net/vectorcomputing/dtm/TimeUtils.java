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

    public static final double DAYS_IN_YEAR = 365;
    public static final double DAYS_IN_MONTH = DAYS_IN_YEAR / 12.0;
    public static final int SECONDS_PER_DAY = 86400;
    public static final double SECONDS_IN_YEAR = DAYS_IN_YEAR * SECONDS_PER_DAY;
    public static final double SECONDS_IN_MONTH = SECONDS_IN_YEAR / 12.0;

    public static final double DOUBLE_NANOS_PER_SECOND = 1000000000.0;
    public static final long LONG_NANOS_PER_SECOND = 1000000000L;

    public static final DateTimeFormatter ISO_LOCAL_TIME_MILLIS = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(MILLI_OF_SECOND, 0, 3, true)
            .toFormatter();

    public static Instant instantFromRFC3339(String dateTime) {
        return Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dateTime));
    }

    /**
     * Aligns the specified timestamp with the nearest, older duration interval integrally offset from the origin time.
     * Minimum supported precision is milliseconds.
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
     * Aligns the specified timestamp with the nearest period interval integrally offset from the origin time.
     * Minimum supported precision is milliseconds.
     * @param timestamp
     * @param origin
     * @param interval
     * @return
     */
    public static Instant alignWithInterval(Instant timestamp, final Instant origin, final Period interval) {
        return alignWithInterval(timestamp, origin, interval, ZoneId.of("UTC"));
    }

    public static Instant alignWithInterval(Instant timestamp, final Instant origin, final Period interval, ZoneId timeZone) {
        Objects.requireNonNull(timestamp, "timestamp must be specified");
        Objects.requireNonNull(origin, "origin must be specified");
        Objects.requireNonNull(interval, "interval must be specified");

        // handle trivial case
        if (origin.equals(timestamp)) {
            return timestamp;
        }

        ZonedDateTime zonedTimestamp = ZonedDateTime.ofInstant(timestamp, timeZone);
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
            // move back one interval
            alignedTime = alignedTime.minus(interval);
        }
        return alignedTime.toInstant();
    }

    /**
     * Returns the total seconds in the specified period duration. The years and months units should be avoided,
     * as they are problematic.
     *
     * TODO This function needs extensive verification as it drives so much of how slices operate.
     * @param pd
     * @return
     */
    public static BigDecimal toSeconds(PeriodDuration pd) {
        BigDecimal period_seconds = toSeconds(pd.getPeriod());
        BigDecimal duration_seconds = toSeconds(pd.getDuration());
        return period_seconds.add(duration_seconds);
    }

    public static BigDecimal toSeconds(Period period) {
        BigDecimal period_seconds = BigDecimal.ZERO;
        if (!period.equals(Period.ZERO)) {
            int years = period.getYears();
            if (years != 0) {
                period_seconds = BigDecimal.valueOf(years)
                        .multiply(BigDecimal.valueOf(SECONDS_IN_YEAR));
            }
            int months = period.getMonths();
            if (months != 0) {
                period_seconds = period_seconds
                        .add(BigDecimal.valueOf(months)
                                .multiply(BigDecimal.valueOf(SECONDS_IN_MONTH)));
            }
            int days = period.getDays();
            if (days != 0) {
                period_seconds = period_seconds
                        .add(BigDecimal.valueOf(days)
                                .multiply(BigDecimal.valueOf(SECONDS_PER_DAY)));
            }
        }
        return period_seconds;
    }

    public static BigDecimal toSeconds(Duration duration) {
        BigDecimal duration_seconds = BigDecimal.valueOf(duration.toNanos());
        duration_seconds.divide(BigDecimal.valueOf(LONG_NANOS_PER_SECOND));
        return duration_seconds;
    }
}
