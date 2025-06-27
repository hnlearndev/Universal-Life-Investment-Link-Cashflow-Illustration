use super::*;

// -----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
#[derive(AsRefStr, Debug, PartialEq, EnumString)]
enum ChrgEnum {
    TP,
    EP,
}

fn get_alloc_chrg_rate_lf(product: &ULEnum, chrg_type: ChrgEnum) -> PolarsResult<LazyFrame> {
    let chrg_type_str = chrg_type.as_ref().to_lowercase();
    let col_name = format!("{}_rate", chrg_type_str);
    let alias_name = format!("{}_alloc_chrg_rate", chrg_type_str);
    let path = "src/database/parquet/ul_alloc_chrg_rate.parquet";
    let lf = LazyFrame::scan_parquet(path, Default::default())?
        .filter(col("product").eq(lit(product.as_ref())))
        .select([col("year"), col(col_name).alias(alias_name)]);
    Ok(lf)
}

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn get_tp_alloc_chrg_rate_lf(product: &ULEnum) -> PolarsResult<LazyFrame> {
    get_alloc_chrg_rate_lf(product, ChrgEnum::TP)
}

pub fn get_ep_alloc_chrg_rate_lf(product: &ULEnum) -> PolarsResult<LazyFrame> {
    get_alloc_chrg_rate_lf(product, ChrgEnum::EP)
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_tp_alloc_chrg_rate_lf_01() {
        let result = get_tp_alloc_chrg_rate_lf(&ULEnum::UVL01)
            .unwrap()
            .filter(col("year").lt_eq(lit(3)).or(col("year").eq(lit(9))))
            .collect()
            .unwrap();

        let expected = df![
            "year" => &[1_i32, 2, 3, 9],
            "tp_alloc_chrg_rate" => &[0.55_f64, 0.40, 0.25, 0.04]
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_tp_alloc_chrg_rate_lf_02() {
        let result = get_tp_alloc_chrg_rate_lf(&ULEnum::UVL01)
            .unwrap()
            .filter(col("year").gt_eq(lit(10)))
            .collect()
            .unwrap();

        let expected = df![
            "year" => &(10..=100).collect::<Vec<i32>>(),
            "tp_alloc_chrg_rate" => &vec![0.02_f64; 91]
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }

    #[test]
    fn test_fn_get_ep_alloc_chrg_rate_lf_01() {
        let result = get_ep_alloc_chrg_rate_lf(&ULEnum::UVL01)
            .unwrap()
            .collect()
            .unwrap();
        let expected = df![
            "year" => &(1..=100).collect::<Vec<i32>>(),
            "ep_alloc_chrg_rate" => &vec![0.02_f64; 100]
        ]
        .unwrap();

        assert!(result.equals(&expected));
    }
}
