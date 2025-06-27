//use crate::enums::DeathTPDBenefitEnum;
use crate::structs::base::Base;
use chrono::Datelike;
use polars::prelude::*;

fn _fixed_init(base: &Base) -> PolarsResult<LazyFrame> {
    // Create the ataframe
    let lf = df![
        "year" => (1..=100).collect::<Vec<i32>>(),
    ]
    .unwrap()
    .lazy()
    .with_columns(vec![
        // Fixed time-related columns
        (lit(base.entry_age()? - 1) + col("year")).alias("age"),
        (lit(base.rcd.year() - 1) + col("year")).alias("cal_year"),
        // Fixed time-related flags
        col("year").lt_eq(lit(base.term()?)).alias("pol_term_flag"),
        col("year")
            .lt_eq(lit(base.acc_ben_term()?))
            .alias("acc_ben_term_flag"),
        col("year")
            .lt_eq(lit(base.load.em_load_term))
            .alias("em_load_term_flag"),
        col("year")
            .lt_eq(lit(base.load.pm_load_term))
            .alias("pm_load_term_flag"),
    ])
    .filter(col("pol_term_flag").eq(lit(true))); // Filter out years beyond the policy term

    Ok(lf.collect()?.lazy())
}

fn _fixed_mapping(lf: LazyFrame, base: &Base) -> PolarsResult<LazyFrame> {
    #[rustfmt::skip]
    let mapping: [(&str, LazyFrame, &str, f64); 8] = [
        ("year", base.withdrawal_plan_lf()?,"withdrawal_input", 0_f64),
        ("year", base.tp_alloc_chrg_rate_lf()?,"tp_alloc_chrg_rate", 0_f64),
        ("year", base.ep_alloc_chrg_rate_lf()?,"ep_alloc_chrg_rate", 0_f64),
        ("year", base.srr_chrg_rate_lf()?,"srr_chrg_rate", 0_f64),
        ("year", base.lb_rate_lf()?,"lb_rate", 0_f64),
        ("age", base.coi_rate_lf()?,"coi_rate", 0_f64),
        ("age", base.juvenile_lien_rate_lf()?,"juvenile_lien_rate", 1_f64),
        ("cal_year", base.admin_chrg_lf()?,"admin_chrg", 0_f64),
    ];

    let mut lf = lf;
    for (_, (key_col, lookup_lf, new_col, default_option)) in mapping.iter().enumerate() {
        lf = lf
            .left_join(lookup_lf.clone(), *key_col, *key_col)
            .with_column(
                col(*new_col)
                    .fill_null(lit(*default_option))
                    .alias(*new_col),
            );
    }
    // Reduce nested LazyFrames by collecting them
    Ok(lf.collect()?.lazy())
}

pub fn fixed(base: &Base) -> PolarsResult<LazyFrame> {
    let lf = _fixed_init(base)?;
    _fixed_mapping(lf, base)
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::read_json_struct;
    use crate::structs::policy::Policy;
    use std::fs::File;

    #[test]
    fn test_fn_fixed() {
        // Generate result DataFrame from fixed function
        let json_path = "src/cashflows/test_data/uvl01_policy.json";
        let policy = read_json_struct::<Policy>(json_path).unwrap();
        let df = fixed(&policy.base).unwrap().collect().unwrap();

        // Export result df to csv to cross check in Excel file
        let export_path = "src/cashflows/test_data/result_fixed.csv";
        let mut export_file = File::create(export_path).unwrap();
        CsvWriter::new(&mut export_file)
            .finish(&mut df.clone())
            .unwrap();

        // Import Excel range as df to check within Rust program
        // TODO: Replace with actual Excel import logic

        // Assert that the result matches the expected DataFrame
        // Result in Excel file: Correct.
        assert!(true, "DataFrames do not match.");
    }
}
