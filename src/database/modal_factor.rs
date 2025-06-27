use super::*;
// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_modal_factor_tuple<T>(product: &T) -> PolarsResult<(f64, f64, f64, f64)>
where
    T: AsRef<str> + std::fmt::Debug,
{
    let path = "src/database/parquet/modal_factor.parquet";
    let df = LazyFrame::scan_parquet(path, Default::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .sort(["mode"], Default::default()) // Default ascending sort
        .select([col("mode"), col("rate")])
        .collect()?;

    // Check if the DataFrame has exactly 4 rows - representing the 4 modes
    if df.height() != 4 {
        return Err(PolarsError::NoData("Expected 4 rows".into()).into());
    }

    // Assume the DataFrame has 4 rows, mode values 0 to 3 in order
    let rates = df
        .column("rate")?
        .f64()?
        .into_no_null_iter()
        .collect::<Vec<f64>>();

    // Return the rates as a tuple: Annual, Semi-annual, Quarterly, Monthly
    Ok((rates[0], rates[1], rates[2], rates[3]))
}
// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::RiderEnum;

    fn get_random_uvl_product() -> ULEnum {
        let random_num = rand::Rng::random_range(&mut rand::rng(), 0..=2);
        match random_num {
            0 => ULEnum::UVL01,
            1 => ULEnum::UVL02,
            _ => ULEnum::UVL03,
        }
    }

    #[test]
    fn test_fn_get_modal_factor_tuple_01() {
        let product = get_random_uvl_product();
        let result = get_modal_factor_tuple(&product).unwrap();
        let expected = (1.0, 0.5, 0.25, 0.083333333);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_fn_get_modal_factor_tuple_02() {
        let result = get_modal_factor_tuple(&RiderEnum::WOP01).unwrap();
        let expected = (1.0, 0.53, 0.27, 0.09);
        assert_eq!(result, expected);
    }
}
