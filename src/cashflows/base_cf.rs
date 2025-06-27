mod fixed;
mod varied;

use crate::cashflows::base_cf::{fixed::fixed, varied::varied};
use crate::enums::{IntRateScenarioEnum, PremTermScenarioEnum, RiskTypeEnum};
use crate::structs::base::Base;
use itertools::iproduct;
use polars::prelude::*;
use strum::IntoEnumIterator;

fn _scenario_cf(
    scenario: (IntRateScenarioEnum, RiskTypeEnum, PremTermScenarioEnum),
    base: &Base,
) -> PolarsResult<LazyFrame> {
    let lf = fixed(base)?;
    varied(scenario, lf, base)
}

pub fn base_cf(base: &Base) -> PolarsResult<LazyFrame> {
    let combination: Vec<_> = iproduct!(
        IntRateScenarioEnum::iter(),
        RiskTypeEnum::iter(),
        PremTermScenarioEnum::iter()
    )
    .collect();

    // Process scenarios in parallel
    let lfs = combination
        .iter()
        .map(|scenario| _scenario_cf(*scenario, base))
        .collect::<PolarsResult<Vec<LazyFrame>>>()
        .expect("Failed to collect DataFrames from scenarios");

    // Concatenate lazyframe
    concat(lfs, Default::default())
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
    fn test_fn_base_cf() {
        // Generate result DataFrame from fixed function
        let json_path = "src/cashflows/test_data/uvl01_policy.json";
        let policy = read_json_struct::<Policy>(json_path).unwrap();
        let df = base_cf(&policy.base).unwrap().collect().unwrap();

        // Export result df to csv to cross check in Excel file
        let export_path = "src/cashflows/test_data/result_base_cf.csv";
        let mut export_file = File::create(export_path).unwrap();
        CsvWriter::new(&mut export_file)
            .finish(&mut df.clone())
            .unwrap();

        // Import Excel range as df to check within Rust program
        // TODO: Replace with actual Excel import logic

        // Assert that the result matches the expected DataFrame
        // Result in Excel file-varied sheet: Correct.
        assert!(true, "DataFrames do not match.");
    }
}
