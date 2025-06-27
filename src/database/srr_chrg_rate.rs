use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_srr_chrg_rate_lf(product: &ULEnum) -> PolarsResult<LazyFrame> {
    let path = "src/database/parquet/ul_srr_chrg_rate.parquet";
    let lf = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .select([col("year"), col("rate").alias("srr_chrg_rate")]);
    Ok(lf)
}
// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_srr_chrg_rate_lf_01() {
        // UVL01
        let result = get_srr_chrg_rate_lf(&ULEnum::UVL01)
            .unwrap()
            .collect()
            .unwrap();

        let mut expected_rates = vec![1_f64, 1.0, 1.0, 1.0, 0.8, 0.6, 0.4, 0.2, 0.1];
        expected_rates.resize(100, 0.0);
        let expected = df![
            "year" => (1_i32..=100).collect::<Vec<i32>>(),
            "srr_chrg_rate" => expected_rates
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_srr_chrg_rate_lf_02() {
        // UVL02
        let result = get_srr_chrg_rate_lf(&ULEnum::UVL02)
            .unwrap()
            .collect()
            .unwrap();

        let mut expected_rates = vec![1_f64, 1.0, 1.0, 0.85, 0.65, 0.45, 0.25, 0.2, 0.1];
        expected_rates.resize(100, 0.0);
        let expected = df![
            "year" => (1_i32..=100).collect::<Vec<i32>>(),
            "srr_chrg_rate" => expected_rates
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_srr_chrg_rate_lf_03() {
        // UVL03
        let result = get_srr_chrg_rate_lf(&ULEnum::UVL03)
            .unwrap()
            .collect()
            .unwrap();

        let mut expected_rates = vec![0.45_f64, 0.4, 0.3, 0.2, 0.1];
        expected_rates.resize(100, 0.0);
        let expected = df![
            "year" => (1_i32..=100).collect::<Vec<i32>>(),
            "srr_chrg_rate" => expected_rates
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }
}
