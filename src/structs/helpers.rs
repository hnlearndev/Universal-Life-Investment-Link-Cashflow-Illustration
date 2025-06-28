use super::*;

//-----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn calculate_age<'a>(
    birthdate: &'a NaiveDate,
    today: &'a NaiveDate,
) -> Result<i32, &'static str> {
    if birthdate > today {
        return Err("Input date cannot be less than birthdate.");
    }
    let mut age = today.year() - birthdate.year();
    if (today.month(), today.day()) < (birthdate.month(), birthdate.day()) {
        age -= 1;
    }
    Ok(age as i32)
}

pub fn calculate_month_age(birthdate: &NaiveDate, today: &NaiveDate) -> Result<i32, &'static str> {
    if birthdate > today {
        return Err("Input date cannot be less than birthdate.");
    }
    let years = today.year() - birthdate.year();
    let months = today.month() as i32 - birthdate.month() as i32;
    let mut total_months = years * 12 + months;
    if today.day() < birthdate.day() {
        total_months -= 1;
    }
    Ok(total_months)
}

//-----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    // Test fn calculate_age
    #[test]
    fn test_fn_calculate_age_01() {
        /*
        DOB year < Input year
        DOB month < Input month
        DOB day < Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2000, 2, 12).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_age(&birthdate, &todate).unwrap();
        assert_eq!(result, 25)
    }

    #[test]
    fn test_fn_calculate_age_02() {
        /*
        DOB year < Input year
        DOB month < Input month
        DOB day > Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2000, 2, 21).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_age(&birthdate, &todate).unwrap();
        assert_eq!(result, 25)
    }

    #[test]
    fn test_fn_calculate_age_03() {
        /*
        DOB year < Input year
        DOB month > Input month
        DOB day > Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2025, 2, 14).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_age(&birthdate, &todate).unwrap();
        assert_eq!(result, 0)
    }

    #[test]
    fn test_fn_calculate_age_04() {
        /*
        DOB year < input year
        Overall age in year is 0
        */
        let birthdate = NaiveDate::from_ymd_opt(2024, 7, 28).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_age(&birthdate, &todate).unwrap();
        assert_eq!(result, 0)
    }

    #[test]
    fn test_fn_calculate_age_05() {
        // DOB > input -> ValueError
        let birthdate = NaiveDate::from_ymd_opt(2030, 7, 28).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_age(&birthdate, &todate);
        // This should return an error, so we should not unwrap but check the error
        assert_eq!(result, Err("Input date cannot be less than birthdate."));
    }

    // Test fn calculate_month_age

    #[test]
    fn test_fn_calculate_month_age_01() {
        /*
        DOB year < Input year
        DOB month < Input month
        DOB day < Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2000, 2, 12).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_month_age(&birthdate, &todate).unwrap();
        // 25 years * 12 + 2 months = 302 months + (day < day, so no -1)
        assert_eq!(result, 302);
    }

    #[test]
    fn test_fn_calculate_month_age_02() {
        /*
        DOB year < Input year
        DOB month < Input month
        DOB day > Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2000, 2, 21).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_month_age(&birthdate, &todate).unwrap();
        // 25 years * 12 + 2 months = 302, but since day > day, -1
        assert_eq!(result, 301);
    }

    #[test]
    fn test_fn_calculate_month_age_03() {
        /*
        DOB year == Input year
        DOB month < Input month
        DOB day < Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2025, 2, 14).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_month_age(&birthdate, &todate).unwrap();
        // 0 years * 12 + 2 months = 2, day < day, so no -1
        assert_eq!(result, 2);
    }

    #[test]
    fn test_fn_calculate_month_age_04() {
        /*
        DOB year == Input year
        DOB month < Input month
        DOB day > Input day
        */
        let birthdate = NaiveDate::from_ymd_opt(2025, 2, 28).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_month_age(&birthdate, &todate).unwrap();
        // 0 years * 12 + 2 months = 2, day > day, so -1
        assert_eq!(result, 1);
    }

    #[test]
    fn test_fn_calculate_month_age_05() {
        /*
        DOB year < Input year
        Overall age in months is less than 12
        */
        let birthdate = NaiveDate::from_ymd_opt(2024, 7, 28).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_month_age(&birthdate, &todate).unwrap();
        // 0 years * 12 + 9 months = 9, day < day, so no -1
        assert_eq!(result, 8);
    }

    #[test]
    fn test_fn_calculate_month_age_06() {
        // DOB > input -> ValueError
        let birthdate = NaiveDate::from_ymd_opt(2030, 7, 28).unwrap();
        let todate = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
        let result = calculate_month_age(&birthdate, &todate);
        assert_eq!(result, Err("Input date cannot be less than birthdate."));
    }
}
