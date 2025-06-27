use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_juvenile_lien_rate_lf(product: &ULEnum) -> PolarsResult<LazyFrame> {
    let path = "src/database/parquet/ul_juvenile_lien_rate.parquet";
    let lf = LazyFrame::scan_parquet(path, Default::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .select([col("age"), col("rate").alias("juvenile_lien_rate")]);
    Ok(lf)
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_juvenile_lien_rate_lf_01() {
        let result = get_juvenile_lien_rate_lf(&ULEnum::UVL01)
            .unwrap()
            .collect()
            .unwrap();
        let expected = df![
            "age" => [0_i32, 1, 2, 3],
            "juvenile_lien_rate" => &[1.0_f64, 1.0, 1.0, 1.0],
        ]
        .unwrap();
        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_juvenile_lien_rate_lf_02() {
        for product in &[ULEnum::UVL02, ULEnum::UVL03, ULEnum::ILP01] {
            let result = get_juvenile_lien_rate_lf(&product)
                .unwrap()
                .collect()
                .unwrap();
            let expected = df![
                "age" => [0_i32, 1, 2, 3],
                "juvenile_lien_rate" => &[0.2_f64, 0.4, 0.6, 0.8],
            ]
            .unwrap();
            assert!(result.equals(&expected));
        }
    }
}
