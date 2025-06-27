use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_lb_rate_lf(product: &ULEnum) -> PolarsResult<LazyFrame> {
    let path = "src/database/parquet/ul_lb_rate.parquet";
    let lf = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .select([col("year"), col("rate").alias("lb_rate")]);
    Ok(lf)
}
// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn get_random_uvl_product() -> ULEnum {
        // Not applicable for UVL03
        let random_num = rand::Rng::random_range(&mut rand::rng(), 0..=1);
        match random_num {
            0 => ULEnum::UVL01,
            _ => ULEnum::ILP01,
        }
    }

    #[test]
    fn test_fn_get_lb_rate_lf_01() {
        let product = get_random_uvl_product();
        let lf = get_lb_rate_lf(&product).unwrap();
        let df = lf.collect().unwrap();
        for year in 1..=20 {
            let rate = df
                .column("lb_rate")
                .unwrap()
                .f64()
                .unwrap()
                .get(year - 1)
                .unwrap();
            let expected = match product {
                ULEnum::UVL01 | ULEnum::ILP01 => {
                    if year % 4 == 0 {
                        0.06_f64 * ((year / 4) as f64)
                    } else {
                        0_f64
                    }
                }
                _ => 0_f64,
            };
            assert!(
                (rate - expected).abs() < 1e-8,
                "year: {}, got: {}, expected: {}",
                year,
                rate,
                expected
            );
        }
    }

    #[test]
    fn test_fn_get_lb_rate_lf_02() {
        let lf = get_lb_rate_lf(&ULEnum::UVL02).unwrap();
        let df = lf.collect().unwrap();
        for year in 1..=20 {
            let rate = df
                .column("lb_rate")
                .unwrap()
                .f64()
                .unwrap()
                .get(year - 1)
                .unwrap();
            let expected = if year % 3 == 0 {
                0.03_f64 * ((year / 3) as f64)
            } else {
                0_f64
            };
            assert!(
                (rate - expected).abs() < 1e-8,
                "year: {}, got: {}, expected: {}",
                year,
                rate,
                expected
            );
        }
    }

    #[test]
    fn test_fn_get_lb_rate_lf_03() {
        let lf = get_lb_rate_lf(&ULEnum::UVL03).unwrap();
        let df = lf.collect().unwrap();
        for year in 1..=20 {
            let rate = df
                .column("lb_rate")
                .unwrap()
                .f64()
                .unwrap()
                .get(year - 1)
                .unwrap();
            let expected = if year % 3 == 0 {
                0.02_f64 + 0.01_f64 * ((year / 3) as f64)
            } else {
                0_f64
            };
            assert!(
                (rate - expected).abs() < 1e-8,
                "year: {}, got: {}, expected: {}",
                year,
                rate,
                expected
            );
        }
    }
}
