/// Number of decimal places an amount carries.
pub const AMOUNT_DECIMALS: u8 = 4;

/// Amount units in one whole unit of currency.
const UNITS_PER_WHOLE: u64 = 10u64.pow(AMOUNT_DECIMALS as u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmountError {
    Malformed,
    TooManyDecimals,
    TooLarge,
}

/// Parses a decimal string into amount units, using integer arithmetic only.
pub fn parse_amount(s: &str) -> Result<u32, AmountError> {
    let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
    let is_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if (whole.is_empty() && fraction.is_empty()) || !is_digits(whole) || !is_digits(fraction) {
        return Err(AmountError::Malformed);
    }
    if fraction.len() > AMOUNT_DECIMALS as usize {
        return Err(AmountError::TooManyDecimals);
    }

    let whole_units = match whole {
        "" => 0,
        digits => digits.parse::<u64>().map_err(|_| AmountError::TooLarge)?,
    };
    let fraction_units = fraction
        .bytes()
        .chain(std::iter::repeat(b'0'))
        .take(AMOUNT_DECIMALS as usize)
        .fold(0, |units, digit| units * 10 + u64::from(digit - b'0'));

    whole_units
        .checked_mul(UNITS_PER_WHOLE)
        .and_then(|units| units.checked_add(fraction_units))
        .and_then(|units| u32::try_from(units).ok())
        .ok_or(AmountError::TooLarge)
}

/// Formats amount units with exactly `AMOUNT_DECIMALS` decimals.
pub fn format_amount(units: u64) -> String {
    format!(
        "{}.{:0width$}",
        units / UNITS_PER_WHOLE,
        units % UNITS_PER_WHOLE,
        width = AMOUNT_DECIMALS as usize
    )
}

/// Formats `minuend - subtrahend`, which may be negative.
///
/// Works from sign and magnitude, so it can't overflow a signed type.
pub fn format_difference(minuend: u64, subtrahend: u64) -> String {
    if minuend >= subtrahend {
        format_amount(minuend - subtrahend)
    } else {
        format!("-{}", format_amount(subtrahend - minuend))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_up_to_four_decimals() {
        assert_eq!(parse_amount("1"), Ok(10_000));
        assert_eq!(parse_amount("1.5"), Ok(15_000));
        assert_eq!(parse_amount("0.0001"), Ok(1));
        assert_eq!(parse_amount("429496.7295"), Ok(u32::MAX));
    }

    #[test]
    fn rejects_invalid_amounts() {
        assert_eq!(parse_amount("1.23456"), Err(AmountError::TooManyDecimals));
        assert_eq!(parse_amount("1.50000"), Err(AmountError::TooManyDecimals));
        assert_eq!(parse_amount("429496.7296"), Err(AmountError::TooLarge));
        assert_eq!(
            parse_amount("99999999999999999999"),
            Err(AmountError::TooLarge)
        );
        for bad in ["-1", "+1", "", ".", "abc", "1.2.3"] {
            assert_eq!(parse_amount(bad), Err(AmountError::Malformed), "{bad:?}");
        }
    }

    #[test]
    fn formats_exactly_four_decimals() {
        assert_eq!(format_amount(15_000), "1.5000");
        assert_eq!(format_amount(1), "0.0001");
        assert_eq!(format_amount(0), "0.0000");
        assert_eq!(format_amount(u64::MAX), "1844674407370955.1615");
    }

    #[test]
    fn formats_signed_difference_without_overflow() {
        assert_eq!(format_difference(200_000, 1_000_000), "-80.0000");
        assert_eq!(format_difference(1_000_000, 200_000), "80.0000");
        assert_eq!(format_difference(5, 5), "0.0000");
        assert_eq!(format_difference(0, u64::MAX), "-1844674407370955.1615");
    }
}
