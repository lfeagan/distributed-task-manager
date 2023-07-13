package net.vectorcomputing.dtm.postgresql;

import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.util.PGInterval;
import org.threeten.extra.PeriodDuration;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;

public class TimeUtils {

    private static final double DAYS_IN_YEAR = 365;
    private static final double DAYS_IN_MONTH = DAYS_IN_YEAR / 12.0;
    private static final int SECONDS_PER_DAY = 86400;
    private static final double SECONDS_IN_YEAR = DAYS_IN_YEAR * SECONDS_PER_DAY;
    private static final double SECONDS_IN_MONTH = SECONDS_IN_YEAR / 12.0;

    private static final double NANOS_PER_SECOND = 1000000000.0;

    static final DateTimeFormatter ISO_LOCAL_TIME_MILLIS = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(MILLI_OF_SECOND, 0, 3, true)
            .toFormatter();

    static DateTimeFormatter postgresqlTimestampWithTzFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME_MILLIS).toFormatter();

    public static String toPostgresqlTimestampWithTz(Instant instant) {
        // ZoneID for UTC is ZoneId.of("UTC+00:00");
        return postgresqlTimestampWithTzFormatter.format(instant.atZone(ZoneId.systemDefault()));
    }

    public static String toPostgresqlInterval(Period period) {
        // Derived from code in PGInterval
        if (period == null) {
            return null;
        }

        return String.format(
                Locale.ROOT,
                "'%d years %d mons %d days'::INTERVAL",
                period.getYears(),
                period.getMonths(),
                period.getDays()
        );
    }

    public static String toMinifiedPostgresqlInterval(Period period) {
        // Derived from code in PGInterval
        if (period == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder("'");
        if (period.getYears() != 0) {
            sb.append(String.format("%d years", period.getYears()));
        }
        if (period.getMonths() != 0) {
            sb.append(String.format("%d mons", period.getMonths()));
        }
        if (period.getDays() != 0) {
            sb.append(String.format("%d days", period.getDays()));
        }

        sb.append("'::INTERVAL");
        return sb.toString();
    }

    public static String toPostgresqlInterval(Duration duration) {
        // Derived from code in PGInterval
        if (duration == null) {
            return null;
        }
        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        df.applyPattern("0.0#####");

        return String.format(
                Locale.ROOT,
                "'%d hours %d mins %s secs'::INTERVAL",
                duration.toHoursPart(), // hours
                duration.toMinutesPart(),
                df.format(duration.toSecondsPart() + duration.getNano() / NANOS_PER_SECOND)
        );
    }

    public static String toMinifiedPostgresqlInterval(Duration duration) {
        // Derived from code in PGInterval
        if (duration == null) {
            return null;
        }
        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        df.applyPattern("0.0#####");

        StringBuilder sb = new StringBuilder("'");
        if (duration.toHoursPart() != 0) {
            sb.append(String.format("%d hours", duration.toHoursPart()));
        }
        if (duration.toMinutesPart() != 0) {
            sb.append(String.format("%d mins", duration.toMinutesPart()));
        }
        if (duration.toSecondsPart() != 0 || duration.getNano() != 0) {
            sb.append(String.format("%s secs",
                    df.format(duration.toSecondsPart() + duration.getNano() / NANOS_PER_SECOND)));
        }

        sb.append("'::INTERVAL");
        return sb.toString();
    }

    public static String toPostgresqlInterval(PeriodDuration pd) {
        // Derived from code in PGInterval
        if (pd == null) {
            return null;
        }
        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        df.applyPattern("0.0#####");

        return String.format(
                Locale.ROOT,
                "'%d years %d mons %d days %d hours %d mins %s secs'::INTERVAL",
                pd.getPeriod().getYears(),
                pd.getPeriod().getMonths(),
                pd.getPeriod().getDays(),
                pd.getDuration().toHoursPart(), // hours
                pd.getDuration().toMinutesPart(),
                df.format(pd.getDuration().toSecondsPart() + pd.getDuration().getNano() / NANOS_PER_SECOND)
        );
    }

    public static String toMinifiedPostgresqlInterval(PeriodDuration pd) {
        // Derived from code in PGInterval
        if (pd == null) {
            return null;
        }
        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        df.applyPattern("0.0#####");

        StringBuilder sb = new StringBuilder("'");
        if (pd.getPeriod().getYears() != 0) {
            sb.append(String.format("%d years", pd.getPeriod().getYears()));
        }
        if (pd.getPeriod().getMonths() != 0) {
            sb.append(String.format("%d mons", pd.getPeriod().getMonths()));
        }
        if (pd.getPeriod().getDays() != 0) {
            sb.append(String.format("%d days", pd.getPeriod().getDays()));
        }
        if (pd.getDuration().toHoursPart() != 0) {
            sb.append(String.format("%d hours", pd.getDuration().toHoursPart()));
        }
        if (pd.getDuration().toMinutesPart() != 0) {
            sb.append(String.format("%d mins", pd.getDuration().toMinutesPart()));
        }
        if (pd.getDuration().toSecondsPart() != 0 || pd.getDuration().getNano() != 0) {
            sb.append(String.format("%s secs",
                    df.format(pd.getDuration().toSecondsPart() + pd.getDuration().getNano() / NANOS_PER_SECOND)));
        }

        sb.append("'::INTERVAL");
        return sb.toString();
    }

    public static PeriodDuration periodDurationFromPGInterval(PGInterval interval) {
        Period p = Period.of(interval.getYears(), interval.getMonths(), interval.getDays());
        Duration d = Duration.ofHours(interval.getHours()).plusMinutes(interval.getMinutes()).plusSeconds(interval.getWholeSeconds()).plusNanos(interval.getMicroSeconds() * 1000L);
        return PeriodDuration.of(p, d);
    }

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
