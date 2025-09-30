package io.pipelines.financial;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;

final class TradingCalendars {
    private TradingCalendars() {}

    static boolean isWeekday(LocalDate d) {
        DayOfWeek dow = d.getDayOfWeek();
        return dow != DayOfWeek.SATURDAY && dow != DayOfWeek.SUNDAY;
    }

    static boolean isUsEquityTradingDay(LocalDate d) {
        if (!isWeekday(d)) return false;
        return !isUsEquityHoliday(d);
    }

    // NYSE holiday calendar (simplified): fixed-date holidays with observed rules; floating Monday holidays; Good Friday.
    static boolean isUsEquityHoliday(LocalDate d) {
        int y = d.getYear();
        // Fixed-date with observed
        if (isObserved(d, Month.JANUARY, 1)) return true;      // New Year's Day
        if (isObserved(d, Month.JUNE, 19)) return true;         // Juneteenth
        if (isObserved(d, Month.JULY, 4)) return true;          // Independence Day
        if (isObserved(d, Month.DECEMBER, 25)) return true;     // Christmas Day

        // Floating Mondays
        if (isNthWeekdayOfMonth(d, 3, DayOfWeek.MONDAY, Month.JANUARY)) return true;   // MLK Day
        if (isNthWeekdayOfMonth(d, 3, DayOfWeek.MONDAY, Month.FEBRUARY)) return true;  // Presidents' Day
        if (isLastWeekdayOfMonth(d, DayOfWeek.MONDAY, Month.MAY)) return true;         // Memorial Day
        if (isNthWeekdayOfMonth(d, 1, DayOfWeek.MONDAY, Month.SEPTEMBER)) return true; // Labor Day
        if (isNthWeekdayOfMonth(d, 4, DayOfWeek.THURSDAY, Month.NOVEMBER)) return true;// Thanksgiving

        // Good Friday (2 days before Easter Sunday)
        LocalDate easter = easterSunday(y);
        if (d.equals(easter.minusDays(2))) return true;
        return false;
    }

    private static boolean isObserved(LocalDate d, Month m, int day) {
        LocalDate date = LocalDate.of(d.getYear(), m, day);
        DayOfWeek dow = date.getDayOfWeek();
        LocalDate observed = date;
        if (dow == DayOfWeek.SATURDAY) observed = date.minusDays(1);
        else if (dow == DayOfWeek.SUNDAY) observed = date.plusDays(1);
        return d.equals(observed);
    }

    private static boolean isNthWeekdayOfMonth(LocalDate d, int n, DayOfWeek dow, Month m) {
        if (d.getMonth() != m || d.getDayOfWeek() != dow) return false;
        int dom = d.getDayOfMonth();
        int ordinal = (dom + 6) / 7; // 1..5
        return ordinal == n;
    }

    private static boolean isLastWeekdayOfMonth(LocalDate d, DayOfWeek dow, Month m) {
        if (d.getMonth() != m || d.getDayOfWeek() != dow) return false;
        int length = d.lengthOfMonth();
        return d.getDayOfMonth() + 7 > length;
    }

    // Anonymous Gregorian algorithm
    private static LocalDate easterSunday(int year) {
        int a = year % 19;
        int b = year / 100;
        int c = year % 100;
        int d = b / 4;
        int e = b % 4;
        int f = (b + 8) / 25;
        int g = (b - f + 1) / 3;
        int h = (19 * a + b - d - g + 15) % 30;
        int i = c / 4;
        int k = c % 4;
        int l = (32 + 2 * e + 2 * i - h - k) % 7;
        int m = (a + 11 * h + 22 * l) / 451;
        int month = (h + l - 7 * m + 114) / 31; // 3=March, 4=April
        int day = ((h + l - 7 * m + 114) % 31) + 1;
        return LocalDate.of(year, month, day);
    }
}

