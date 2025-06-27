use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_admin_chrg_lf(product: &ULEnum) -> PolarsResult<LazyFrame> {
    let parquet_path = "src/database/parquet/ul_admin_chrg.parquet";
    let lf = LazyFrame::scan_parquet(parquet_path, Default::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .select([col("cal_year"), col("amount").alias("admin_chrg")]);
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
        let random_num = rand::Rng::random_range(&mut rand::rng(), 0..=2);
        match random_num {
            0 => ULEnum::UVL02,
            1 => ULEnum::UVL03,
            _ => ULEnum::ILP01,
        }
    }

    #[test]
    fn test_fn_get_admin_chrg_lf_01() {
        let result = get_admin_chrg_lf(&ULEnum::UVL01)
            .unwrap()
            .collect()
            .unwrap();

        let expected = df![
            "cal_year" => (2016..=2115).collect::<Vec<i32>>(),
            "admin_chrg" => vec![25_000_f64; 2115 - 2016 + 1]
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_admin_chrg_lf_02() {
        for product in [ULEnum::UVL02, ULEnum::UVL03, ULEnum::ILP01] {
            let result = get_admin_chrg_lf(&product).unwrap().collect().unwrap();

            let expected = df![
                "cal_year" => (2024..=2123).collect::<Vec<i32>>(),
                "admin_chrg" => (0..(2123-2024+1)).map(|i| f64::min(40000.0 + 2000.0 * i as f64, 66000.0)).collect::<Vec<f64>>(),
            ]
            .unwrap();

            assert!(result.equals(&expected));
        }
        let result = get_admin_chrg_lf(&ULEnum::UVL02)
            .unwrap()
            .collect()
            .unwrap();

        let expected = df![
            "cal_year" => (2024..=2123).collect::<Vec<i32>>(),
            "admin_chrg" => (0..(2123-2024+1)).map(|i| f64::min(40000.0 + 2000.0 * i as f64, 66000.0)).collect::<Vec<f64>>(),
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }
}
