use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_coi_rate_lf(product: &ULEnum, gender: &GenderEnum) -> PolarsResult<LazyFrame> {
    // Load the Parquet file into a LazyFrame
    let product_str = product.as_ref();
    let gender_num = *gender as i32;
    let path = "src/database/parquet/ul_coi_rate.parquet";
    let lf = LazyFrame::scan_parquet(path, Default::default())?
        .filter(
            col("product")
                .eq(lit(product_str))
                .and(col("gender").eq(lit(gender_num))),
        )
        .select([col("age"), col("rate").alias("coi_rate")]);
    Ok(lf)
}
// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_coi_rate_lf_01() {
        let result = get_coi_rate_lf(&ULEnum::UVL01, &GenderEnum::Male)
            .unwrap()
            .filter(col("age").eq(lit(0)).or(col("age").eq(lit(26))))
            .collect()
            .unwrap();

        let expected = df![
            "age" => &[0_i32, 26],
            "coi_rate" => &[0.00263_f64, 0.00172]
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_coi_rate_lf_02() {
        let result = get_coi_rate_lf(&ULEnum::UVL01, &GenderEnum::Female)
            .unwrap()
            .filter(
                col("age")
                    .eq(lit(0))
                    .or(col("age").eq(lit(42)))
                    .or(col("age").eq(lit(99))),
            )
            .collect()
            .unwrap();

        let expected = df![
            "age" => &[0_i32, 42, 99],
            "coi_rate" => &[0.00187999999999999_f64, 0.00297999999999998, 1.0]
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }
}
