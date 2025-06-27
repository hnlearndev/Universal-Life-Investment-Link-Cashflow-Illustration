use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_prem_rate(product: &ULEnum, gender: &GenderEnum, age: &i32) -> PolarsResult<f64> {
    let path = "src/database/parquet/ul_prem_rate.parquet";
    let lf = LazyFrame::scan_parquet(path, Default::default())?;
    let rate = lf
        .filter(
            col("product")
                .eq(lit(product.as_ref()))
                .and(col("gender").eq(lit(*gender as i32)))
                .and(col("age").eq(lit(*age))),
        )
        .select([col("rate")])
        .collect()?
        .column("rate")?
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
    fn test_fn_get_prem_rate_01() {
        let result = get_prem_rate(&ULEnum::UVL01, &GenderEnum::Male, &0).unwrap();
        assert_eq!(result, 10.6128000_f64);
    }

    #[test]
    fn test_fn_get_prem_rate_02() {
        let result = get_prem_rate(&ULEnum::UVL01, &GenderEnum::Male, &34).unwrap();
        assert_eq!(result, 21.0406800_f64);
    }

    #[test]
    fn test_fn_get_prem_rate_03() {
        let result = get_prem_rate(&ULEnum::UVL01, &GenderEnum::Female, &0).unwrap();
        assert_eq!(result, 10.56_f64);
    }
}
