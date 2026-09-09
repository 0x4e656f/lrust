//! PostgreSQL binary temporal values. Integer arithmetic covers PG's full
//! finite range without Chrono's narrower range or wrapping 24:00 to midnight.
const DAY: i64 = 86_400_000_000;

fn civil_date(pg_days: i64) -> String {
    // Gregorian civil-from-days, with Euclidean eras for dates before year 0.
    // Shift the PostgreSQL 2000 epoch to a March-based Gregorian epoch.
    let z = pg_days + 10_957 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    // Preserve the existing ISO/Chrono astronomical-year convention (year 0
    // is 1 BC), including a sign for years outside 0000..9999.
    let year = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else if year < 0 {
        format!("-{:04}", -year)
    } else {
        format!("+{year}")
    };
    format!("{year}-{month:02}-{day:02}")
}

pub fn date(days: i32) -> String {
    match days {
        i32::MAX => "infinity".into(),
        i32::MIN => "-infinity".into(),
        days => civil_date(days.into()),
    }
}

pub fn time(micros: i64) -> Result<String, String> {
    if !(0..=DAY).contains(&micros) {
        return Err("PostgreSQL TIME is outside 00:00:00..24:00:00".into());
    }
    let seconds = micros / 1_000_000;
    let fraction = micros % 1_000_000;
    let mut result = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    // Same fractional formatting as Chrono %.f / SecondsFormat::AutoSi.
    if fraction != 0 {
        if fraction % 1000 == 0 {
            result.push_str(&format!(".{:03}", fraction / 1000));
        } else {
            result.push_str(&format!(".{fraction:06}"));
        }
    }
    Ok(result)
}

pub fn timestamp(micros: i64, utc: bool) -> Result<String, String> {
    match micros {
        i64::MAX => return Ok("infinity".into()),
        i64::MIN => return Ok("-infinity".into()),
        _ => (),
    }
    Ok(format!(
        "{}{}{}{}",
        civil_date(micros.div_euclid(DAY)),
        if utc { "T" } else { " " },
        time(micros.rem_euclid(DAY))?,
        if utc { "Z" } else { "" }
    ))
}

pub fn timetz(micros: i64, seconds_west: i32) -> Result<String, String> {
    // PostgreSQL permits offsets strictly less than 16 hours.
    if !(-57_599..=57_599).contains(&seconds_west) {
        return Err("PostgreSQL TIMETZ timezone offset is out of range".into());
    }
    let offset = seconds_west.unsigned_abs();
    let mut result = format!(
        "{}{}{:02}:{:02}",
        time(micros)?,
        if seconds_west > 0 { '-' } else { '+' },
        offset / 3600,
        offset / 60 % 60
    );
    if !offset.is_multiple_of(60) {
        result.push_str(&format!(":{:02}", offset % 60));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn epochs_leap_days_and_full_range() {
        assert_eq!(date(0), "2000-01-01");
        assert_eq!(date(-10_957), "1970-01-01");
        assert_eq!(date(59), "2000-02-29");
        assert_eq!(date(-1), "1999-12-31");
        assert_eq!(date(-730_485), "0000-01-01");
        assert_eq!(date(i32::MAX), "infinity");
        assert_eq!(date(i32::MIN), "-infinity");
        assert!(date(2_145_031_948).starts_with("+5874897"));
        assert_eq!(timestamp(-1, true).unwrap(), "1999-12-31T23:59:59.999999Z");
        assert_eq!(timestamp(i64::MAX, false).unwrap(), "infinity");
        assert_eq!(timestamp(i64::MIN, true).unwrap(), "-infinity");
        assert!(timestamp(i64::MAX - 1, true).is_ok());
        assert!(timestamp(i64::MIN + 1, false).is_ok());
    }
    #[test]
    fn end_of_day_fractions_and_offsets() {
        assert_eq!(time(DAY).unwrap(), "24:00:00");
        assert_eq!(time(12_345_000).unwrap(), "00:00:12.345");
        assert_eq!(time(12_000_001).unwrap(), "00:00:12.000001");
        assert!(time(-1).is_err());
        assert!(time(DAY + 1).is_err());
        assert_eq!(timetz(DAY, -19800).unwrap(), "24:00:00+05:30");
        assert_eq!(timetz(0, 1).unwrap(), "00:00:00-00:00:01");
        assert!(timetz(0, i32::MIN).is_err());
    }
}
