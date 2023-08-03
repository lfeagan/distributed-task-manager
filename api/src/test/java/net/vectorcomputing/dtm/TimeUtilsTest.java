package net.vectorcomputing.dtm;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.*;

import static net.vectorcomputing.dtm.TimeUtils.instantFromRFC3339;

public class TimeUtilsTest {

    private static final Instant YEAR_2000 = instantFromRFC3339("2000-01-01T00:00:00Z");
    private static final Instant YEAR_3000 = instantFromRFC3339("3000-01-01T00:00:00Z");

    @Test
    public void fineGrainedHourIntervals() {
        final Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        {
            final Instant oneHourAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T01:23:45Z"), origin, Duration.parse("PT1H"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(oneHourAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 1);
            Assert.assertEquals(zoned.getMinute(), 0);
            Assert.assertEquals(zoned.getSecond(), 0);
        } {
            final Instant twoHourAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T05:23:45Z"), origin, Duration.parse("PT2H"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(twoHourAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 4);
            Assert.assertEquals(zoned.getMinute(), 0);
            Assert.assertEquals(zoned.getSecond(), 0);
        } {
            final Instant threeHourAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T05:23:45Z"), origin, Duration.parse("PT3H"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(threeHourAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 3);
            Assert.assertEquals(zoned.getMinute(), 0);
            Assert.assertEquals(zoned.getSecond(), 0);
        }
    }

    @Test
    public void fineGrainedMinuteIntervals() {
        final Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        {
            final Instant oneHourAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T01:23:45Z"), origin, Duration.parse("PT1M"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(oneHourAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 1);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 0);
        } {
            final Instant twoHourAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T05:23:45Z"), origin, Duration.parse("PT2M"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(twoHourAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 5);
            Assert.assertEquals(zoned.getMinute(), 22);
            Assert.assertEquals(zoned.getSecond(), 0);
        } {
            final Instant threeHourAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T07:23:45Z"), origin, Duration.parse("PT3M"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(threeHourAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 7);
            Assert.assertEquals(zoned.getMinute(), 21);
            Assert.assertEquals(zoned.getSecond(), 0);
        }
    }

    @Test
    public void fineGrainedSecondIntervals() {
        final Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        {
            final Instant oneSecondAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T01:23:45Z"), origin, Duration.parse("PT1S"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(oneSecondAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 1);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 45);
        } {
            final Instant twoSecondAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T05:23:45Z"), origin, Duration.parse("PT2S"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(twoSecondAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 5);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 44);
        } {
            final Instant fiveSecondAligned = TimeUtils.alignWithInterval(instantFromRFC3339("2000-01-01T07:23:47Z"), origin, Duration.parse("PT5S"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(fiveSecondAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 7);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 45);
        }
    }

    @Test
    public void fineGrainedMillisecondIntervals() {
        final Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        final Instant reference = instantFromRFC3339("2000-01-01T01:23:45.567Z");
        // confirm milliseconds were parsed in the reference time
        Assert.assertEquals(reference.getNano(), 567000000);
        {
            final Instant oneTenthSecondAligned = TimeUtils.alignWithInterval(reference, origin, Duration.parse("PT0.1S"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(oneTenthSecondAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 1);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 45);
            Assert.assertEquals(zoned.getNano(), 500000000);
        } {
            final Instant twoTenthSecondAligned = TimeUtils.alignWithInterval(reference, origin, Duration.parse("PT0.2S"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(twoTenthSecondAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 1);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 45);
            Assert.assertEquals(zoned.getNano(), 400000000);
        } {
            final Instant halfSecondAligned = TimeUtils.alignWithInterval(reference, origin, Duration.parse("PT0.5S"));
            ZonedDateTime zoned = ZonedDateTime.ofInstant(halfSecondAligned, ZoneId.of("UTC"));
            Assert.assertEquals(zoned.getHour(), 1);
            Assert.assertEquals(zoned.getMinute(), 23);
            Assert.assertEquals(zoned.getSecond(), 45);
            Assert.assertEquals(zoned.getNano(), 500000000);
        }
    }

    @Test
    public void oldTime() {
        Instant timestamp = instantFromRFC3339("1973-12-03T15:53:58Z");
        Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        Period window = Period.parse("P100Y");
        Instant bucketStart = TimeUtils.alignWithInterval(timestamp, origin, window);
        System.out.println(bucketStart);
        Assert.assertEquals(bucketStart, instantFromRFC3339("1900-01-01T00:00:00Z"), "start");
    }

    @Test
    public void currentTime() {
        Instant timestamp = instantFromRFC3339("2023-12-03T15:53:58Z");
        Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        Period window = Period.parse("P100Y");
        Instant bucketStart = TimeUtils.alignWithInterval(timestamp, origin, window);
        System.out.println(bucketStart);
        Assert.assertEquals(bucketStart, instantFromRFC3339("2000-01-01T00:00:00Z"), "start");
    }

    @Test
    public void starTrekTime() {
        Instant timestamp = instantFromRFC3339("4123-12-03T15:53:58Z");
        Instant origin = instantFromRFC3339("2000-01-01T00:00:00Z");
        Period window = Period.parse("P100Y");
        Instant bucketStart = TimeUtils.alignWithInterval(timestamp, origin, window);
        System.out.println(bucketStart);
        Assert.assertEquals(bucketStart, instantFromRFC3339("4100-01-01T00:00:00Z"), "start");
    }
}
