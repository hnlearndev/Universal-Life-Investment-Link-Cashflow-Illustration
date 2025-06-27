use super::*;

// -----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
fn get_int_rate(product: &ULEnum, scenario: IntRateScenarioEnum) -> PolarsResult<f64> {
    let path = "src/database/parquet/ul_int_rate.parquet";
    let rate = LazyFrame::scan_parquet(path, Default::default())?
        .filter(
            col("product")
                .eq(lit(product.as_ref()))
                .and(col("scenario").eq(lit(scenario as i32))),
        )
        .collect()?
        .column("rate")?
        .get(0)?
        .try_extract::<f64>()?;
    Ok(rate)
}
// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_hir(product: &ULEnum) -> PolarsResult<f64> {
    get_int_rate(product, IntRateScenarioEnum::High)
}

pub fn get_lir(product: &ULEnum) -> PolarsResult<f64> {
    get_int_rate(product, IntRateScenarioEnum::Low)
}

pub fn get_gir(product: &ULEnum) -> PolarsResult<f64> {
    get_int_rate(product, IntRateScenarioEnum::Guaranteed)
}
// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn get_random_uvl_product() -> ULEnum {
        let random_num = rand::Rng::random_range(&mut rand::rng(), 0..=2);
        match random_num {
            0 => ULEnum::UVL01,
            1 => ULEnum::UVL02,
            _ => ULEnum::UVL03,
        }
    }

    #[test]
    fn test_fn_int_rate_01() {
        // UVL high interest rate
        let product = get_random_uvl_product();
        let result = (
            get_hir(&product).unwrap(),
            get_lir(&product).unwrap(),
            get_gir(&product).unwrap(),
        );
        let expected = (0.07f64, 0.05f64, 0.02_f64);
        assert_eq!(result, expected);
    }
}
