use super::*;

// -----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
#[derive(AsRefStr, Debug, PartialEq, EnumString)]
pub enum AgeValidationTypeEnum {
    #[strum(serialize = "min_entry_age")]
    Min,
    #[strum(serialize = "max_entry_age")]
    Max,
    #[strum(serialize = "maturity_age")]
    Maturity,
}

// -1 result indicates that the product requires manual intervention
pub fn get_age_validation<T>(product: &T, validate_type: AgeValidationTypeEnum) -> PolarsResult<i32>
where
    T: AsRef<str> + std::fmt::Debug,
{
    let col_name = validate_type.as_ref();
    let path = "src/database/parquet/age_validation.parquet";
    let age = LazyFrame::scan_parquet(path, Default::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .select([col(col_name)])
        .collect()?
        .column(col_name)
        .unwrap() // Certainly there is a column with the name "col_name" after selecting it
        .get(0)?
        .try_extract::<i32>()?;
    Ok(age)
}

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_min_entry_age<T>(product: &T) -> PolarsResult<i32>
where
    T: AsRef<str> + std::fmt::Debug,
{
    get_age_validation(product, AgeValidationTypeEnum::Min)
}

pub fn get_max_entry_age<T>(product: &T) -> PolarsResult<i32>
where
    T: AsRef<str> + std::fmt::Debug,
{
    get_age_validation(product, AgeValidationTypeEnum::Max)
}

pub fn get_maturity_age<T>(product: &T) -> PolarsResult<i32>
where
    T: AsRef<str> + std::fmt::Debug,
{
    get_age_validation(product, AgeValidationTypeEnum::Maturity)
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
    fn test_fn_get_min_entry_age_01() {
        let product = get_random_uvl_product();
        let result = get_min_entry_age(&product).unwrap();
        assert_eq!(result, -1_i32);
    }

    #[test]
    fn test_fn_get_min_entry_age_02() {
        let result = get_min_entry_age(&RiderEnum::ADD01).unwrap();
        assert_eq!(result, 18_i32);
    }

    // Test fn get_max_entry_age
    #[test]
    fn test_fn_get_max_entry_age_01() {
        let product = get_random_uvl_product();
        let result = get_max_entry_age(&product).unwrap();
        assert_eq!(result, 60_i32);
    }

    #[test]
    fn test_fn_get_max_entry_age_02() {
        let result = get_max_entry_age(&RiderEnum::WOP01).unwrap();
        assert_eq!(result, 60_i32);
    }

    // Test fn get_maturity_age
    #[test]
    fn test_fn_get_maturity_age_01() {
        let product = get_random_uvl_product();
        let result = get_maturity_age(&product).unwrap();
        assert_eq!(result, -1_i32);
    }

    #[test]
    fn test_fn_get_maturity_age_02() {
        let result = get_maturity_age(&RiderEnum::CIR02).unwrap();
        assert_eq!(result, 75_i32);
    }
}
