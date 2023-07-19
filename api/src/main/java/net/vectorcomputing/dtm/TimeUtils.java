package net.vectorcomputing.dtm;

import org.apache.commons.lang3.tuple.Pair;
import org.threeten.extra.PeriodDuration;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;

public final class TimeUtils {

    public static final double DAYS_IN_YEAR = 365;
    public static final double DAYS_IN_MONTH = DAYS_IN_YEAR / 12.0;
    public static final int SECONDS_PER_DAY = 86400;
    public static final double SECONDS_IN_YEAR = DAYS_IN_YEAR * SECONDS_PER_DAY;
    public static final double SECONDS_IN_MONTH = SECONDS_IN_YEAR / 12.0;

    public static final double NANOS_PER_SECOND = 1000000000.0;

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

    public static Instant alignWithDuration(Instant timestamp, PeriodDuration periodDuration) {
        return computeBucketStartAndEnd(timestamp, Instant.ofEpochMilli(0), periodDuration).getLeft();
    }

    /**
     *
     * @param timestamp in epoch millis
     * @param windowOrigin in epoch millis
     * @param windowSize in millis
     * @return
     */
    public static Pair<Instant, Instant> computeBucketStartAndEnd(Instant timestamp, Instant windowOrigin, PeriodDuration windowSize) {
        // buckets must be second aligned, cannot be less than one second
        timestamp = timestamp.truncatedTo(ChronoUnit.SECONDS);
        windowOrigin = windowOrigin.truncatedTo(ChronoUnit.SECONDS);
//        int offsetSeconds = (int) windowOrigin.until(timestamp, ChronoUnit.SECONDS);
//        Seconds offsetFromOrigin = Seconds.of(offsetSeconds);
//        Seconds intervals = offsetFromOrigin.dividedBy(windowSize.abs().getAmount());
        long delta_t = timestamp.getEpochSecond() - windowOrigin.getEpochSecond();
        int interval = (int) (delta_t / toSeconds(windowSize));

        LocalDateTime start = LocalDateTime.ofInstant(windowOrigin, ZoneId.of("UTC"));
        if (delta_t < 0) {
            interval += 1;
            for (int i=0; i < interval; ++i) {
                start = start.minus(windowSize);
            }
        } else {
            start = start.plus(windowSize.multipliedBy(interval));
        }

        Instant end = start.plus(windowSize).toInstant(ZoneOffset.UTC);
        return Pair.of(start.toInstant(ZoneOffset.UTC), end);
    }

    /**
     * Returns the total seconds in the specified period duration. The years and months units should be avoided,
     * as they are problematic.
     *
     * TODO This function needs extensive verification as it drives so much of how slices operate.
     * @param pd
     * @return
     */
    public static long toSeconds(PeriodDuration pd) {
        BigDecimal period_seconds = BigDecimal.ZERO;
        if (!pd.getPeriod().equals(Period.ZERO)) {
            int years = pd.getPeriod().getYears();
            if (years != 0) {
                period_seconds = BigDecimal.valueOf(years)
                        .multiply(BigDecimal.valueOf(SECONDS_IN_YEAR));
            }
            int months = pd.getPeriod().getMonths();
            if (months != 0) {
                period_seconds = period_seconds
                        .add(BigDecimal.valueOf(months)
                                .multiply(BigDecimal.valueOf(SECONDS_IN_MONTH)));
            }
            int days = pd.getPeriod().getDays();
            if (days != 0) {
                period_seconds = period_seconds
                        .add(BigDecimal.valueOf(days)
                                .multiply(BigDecimal.valueOf(SECONDS_PER_DAY)));
            }
        }
        BigDecimal seconds = period_seconds.add(BigDecimal.valueOf(pd.getDuration().toMillis()/1000));
        return seconds.longValue();
    }
}
