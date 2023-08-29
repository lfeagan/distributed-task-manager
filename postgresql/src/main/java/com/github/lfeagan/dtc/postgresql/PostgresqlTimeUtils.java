package com.github.lfeagan.dtc.postgresql;

import org.postgresql.util.PGInterval;
import org.threeten.extra.PeriodDuration;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;

public class PostgresqlTimeUtils {

    private static final double NANOS_PER_SECOND_DOUBLE = 1000000000.0;
    private static final long NANOS_PER_SECOND_LONG = 1000000000L;

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
                df.format(duration.toSecondsPart() + duration.getNano() / NANOS_PER_SECOND_DOUBLE)
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
                    df.format(duration.toSecondsPart() + duration.getNano() / NANOS_PER_SECOND_DOUBLE)));
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
                df.format(pd.getDuration().toSecondsPart() + pd.getDuration().getNano() / NANOS_PER_SECOND_DOUBLE)
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
                    df.format(pd.getDuration().toSecondsPart() + pd.getDuration().getNano() / NANOS_PER_SECOND_DOUBLE)));
        }

        sb.append("'::INTERVAL");
        return sb.toString();
    }

    public static PeriodDuration periodDurationFromPGInterval(PGInterval interval) {
        Period p = Period.of(interval.getYears(), interval.getMonths(), interval.getDays());
        Duration d = Duration.ofHours(interval.getHours()).plusMinutes(interval.getMinutes()).plusSeconds(interval.getWholeSeconds()).plusNanos(interval.getMicroSeconds() * 1000L);
        return PeriodDuration.of(p, d);
    }

}
