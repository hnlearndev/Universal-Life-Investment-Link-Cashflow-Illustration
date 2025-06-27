use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_extra_prem_rate(
    product: &ULEnum,
    gender: &GenderEnum,
    age: &i32,
    term: &i32,
) -> PolarsResult<f64> {
    let path = "src/database/parquet/ul_extra_prem_rate.parquet";

    let rate = LazyFrame::scan_parquet(path, Default::default())?
        .filter(
            col("product")
                .eq(lit(product.as_ref()))
                .and(col("gender").eq(lit(*gender as i32)))
                .and(col("age").eq(lit(*age)))
                .and(col("term").eq(lit(i32::min(*term, 99)))), // Or eslse it wiil fail at age = 0 with term 100 - Suspect might be a flasw in orginianl pricing design
        )
        .select([col("rate")])
        .collect()?
        .column("rate")
        .unwrap() // Certainly there is a column  with the name "rate" after selecting it
        .get(0)?
        .try_extract::<f64>()?;
    Ok(rate)
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_extra_prem_rate_01() {
        let result = get_extra_prem_rate(&ULEnum::UVL01, &GenderEnum::Male, &10, &39).unwrap();
        assert_eq!(result, 1.53_f64);
    }

    #[test]
    fn test_fn_get_extra_prem_rate_02() {
        let result = get_extra_prem_rate(&ULEnum::UVL01, &GenderEnum::Female, &20, &13).unwrap();
        assert_eq!(result, 1.19_f64);
    }

    #[test]
    fn test_fn_get_extra_prem_rate_03() {
        // Test with age 0 and term 100
        let result = get_extra_prem_rate(&ULEnum::UVL01, &GenderEnum::Female, &0, &100).unwrap();
        assert_eq!(result, 0.91_f64); // Obtain the one at age 99
    }
}
