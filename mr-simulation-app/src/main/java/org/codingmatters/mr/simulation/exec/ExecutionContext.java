package org.codingmatters.mr.simulation.exec;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExecutionContext {

    public static final int UNPARSABLE_DATE_FIELD_VALUE = -1;

    public static void setup(Bindings bindings) {
        bindings.put("dateYear", (Function<String, Integer>) (str) -> dateField(ChronoField.YEAR, str));
        bindings.put("dateMonth", (Function<String, Integer>) (str) -> dateField(ChronoField.MONTH_OF_YEAR, str));
        bindings.put("dateDay", (Function<String, Integer>) (str) -> dateField(ChronoField.DAY_OF_MONTH, str));
        bindings.put("dateHour", (Function<String, Integer>) (str) -> dateField(ChronoField.HOUR_OF_DAY, str));
        bindings.put("dateMinute", (Function<String, Integer>) (str) -> dateField(ChronoField.MINUTE_OF_HOUR, str));
        bindings.put("dateSecond", (Function<String, Integer>) (str) -> dateField(ChronoField.SECOND_OF_MINUTE, str));

        bindings.put("dateFormat", (BiFunction<String, String, String>) (str, format) -> dateFormat(str, format));
    }

    static public int dateField(ChronoField field, String dateString) {
        try {
            return parseDate(dateString).get(field);
        } catch (DateTimeParseException e) {
            return UNPARSABLE_DATE_FIELD_VALUE;
        }
    }

    /**
     * https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns
     */
    static public String dateFormat(String dateString, String format) {
        try {
            return parseDate(dateString).format(DateTimeFormatter.ofPattern(format));
        } catch (Exception e) {
            return "DATE-FORMAT-ERROR : " + e.getMessage();
        }
    }

    private static LocalDateTime parseDate(String dateString) {
        return LocalDateTime.parse(dateString, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
