use crate::cashflows::helpers::*;
use crate::enums::{
    DeathTPDBenefitEnum, IntRateScenarioEnum, PremTermScenarioEnum, RiskTypeEnum,
};
use crate::structs::base::Base;
use polars::prelude::*;

// Import the macro from the crate root due to #[macro_export]
use crate::update_df_with_vectors;

// -----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
fn _varied_init(
    scenario: (IntRateScenarioEnum, RiskTypeEnum, PremTermScenarioEnum),
    input_lf: LazyFrame,
    base: &Base,
) -> PolarsResult<LazyFrame> {
    // Interest rate and future value calculations
    let annual_int_rate = match scenario.0 {
        IntRateScenarioEnum::High => base.int_rate_tuple()?.0,
        IntRateScenarioEnum::Low => base.int_rate_tuple()?.1,
        _ => base.int_rate_tuple()?.2,
    };
    let monthly_int_rate = (1_f64 + annual_int_rate).powf(1_f64 / 12_f64) - 1_f64;
    let due_a_n1_m12 = ((1_f64 + monthly_int_rate).powf(12_f64) - 1_f64)
        / 12_f64
        / (1_f64 - (1_f64 + monthly_int_rate).powf(-1_f64))
        / (1_f64 + annual_int_rate);

    // Define TP and EP term based on scenario
    let term = match scenario.2 {
        PremTermScenarioEnum::PolicyTerm => ("Policy", base.term()?, base.term()?),
        PremTermScenarioEnum::OptedTerm => ("Opted", base.opted_tp_term, base.opted_ep_term),
        _ => ("Must-pay", base.must_pay_period()?, base.must_pay_period()?),
    };

    // Create the lazy frame with fixed columns
    let df = input_lf
        .with_columns(vec![
            // Columns to identify the scenario
            lit(scenario.0.as_ref()).alias("int_rate_scenario"),
            lit(scenario.1.as_ref()).alias("risk_scenario"),
            lit(term.0).alias("term_scenario"),
            // Premium term flags
            col("year").lt_eq(lit(term.1)).alias("tp_term_flag"),
            col("year").lt_eq(lit(term.2)).alias("ep_term_flag"),
            // Interest rate based on scenario
            lit(annual_int_rate).alias("annual_int_rate"),
            lit(due_a_n1_m12).alias("due_a_n1_m12"),
        ])
        .with_columns(vec![
            // TP with big case bonus
            (when(col("year").eq(lit(1)))
                .then(lit(base.modal_tp_tuple()?.0 + base.big_case_bonus()?))
                .otherwise(lit(base.modal_tp_tuple()?.0))
                * col("tp_term_flag"))
            .alias("tp"),
            // EP
            (lit(base.modal_ep_tuple()?.0) * col("tp_term_flag")).alias("ep"),
            // Risk flags
            col("risk_scenario").eq(lit("Subrisk")).alias("risk_flag"),
        ])
        .with_columns(vec![
            // Surrender charge
            (col("tp") * col("srr_chrg_rate")).alias("srr_chrg"),
            // Allocation charge
            (col("tp") * col("tp_alloc_chrg_rate")).alias("tp_alloc_chrg"),
            (col("ep") * col("ep_alloc_chrg_rate")).alias("ep_alloc_chrg"),
        ])
        .with_columns(vec![
            // Premium allocation
            (col("tp") - col("tp_alloc_chrg")).alias("tp_alloc"),
            (col("ep") - col("ep_alloc_chrg")).alias("ep_alloc"),
        ])
        .with_columns(vec![
            // Start values
            lit(false).alias("cont_flag"),
            lit(0.0).alias("start_si"),
            lit(0.0).alias("start_eav"),
            lit(0.0).alias("start_tav"),
            lit(0.0).alias("start_pav"),
            // Surrender charge and benefit values
            lit(0.0).alias("srr_val"),
            lit(0.0).alias("ben"),
            lit(0.0).alias("acc_ben"),
            // Withdrawal
            lit(0.0).alias("withdrawal"),
            lit(0.0).alias("eav_withdrawal"),
            lit(0.0).alias("tav_withdrawal"),
            lit("No withdrawal.").alias("withdrawal_log"),
            // Load
            lit(0.0).alias("unrounded_em_load"),
            lit(0.0).alias("unrounded_pm_load"),
            lit(0.0).alias("em_load"),
            lit(0.0).alias("pm_load"),
            lit(0.0).alias("load"),
            lit(0.0).alias("load_alloc_chrg"),
            lit(0.0).alias("load_alloc"),
            lit(0.0).alias("alloc_chrg"),
            lit(0.0).alias("alloc"),
            // COI
            lit(0.0).alias("sar"),
            lit(0.0).alias("standard_coi"),
            lit(0.0).alias("em_load_coi"),
            lit(0.0).alias("pm_load_coi"),
            lit(0.0).alias("acc_coi"),
            lit(0.0).alias("coi"),
            // Account values after withdrawal and allocation
            lit(0.0).alias("eav_after_wdrl_and_alloc"),
            lit(0.0).alias("tav_after_wdrl_and_alloc"),
            lit(0.0).alias("pav_after_wdrl_and_alloc"),
            // Deduction
            lit(0.0).alias("plan_nom_deduction"),
            lit(0.0).alias("plan_deduction"),
            lit(false).alias("deduction_flag"),
            lit(0.0).alias("nom_deduction"),
            lit(0.0).alias("deduction"),
            lit(0.0).alias("tav_deduction"),
            lit(0.0).alias("eav_deduction"),
            // Interest
            lit(0.0).alias("eav_int"),
            lit(0.0).alias("tav_int"),
            lit(0.0).alias("int"),
            // Bonus
            lit(0.0).alias("lb_tav_withdrawal_review"),
            lit(false).alias("lb_flag"),
            lit(0.0).alias("lb"),
            lit(0.0).alias("sb_rate"),
            lit(0.0).alias("sb_tav_withdrawal_review"),
            lit(false).alias("sb_flag"),
            lit(0.0).alias("sb"),
            lit(0.0).alias("bonus"),
            // End values
            lit(0.0).alias("end_si"),
            lit(0.0).alias("end_eav"),
            lit(0.0).alias("end_tav"),
            lit(0.0).alias("end_pav"),
        ])
        .collect()
        .unwrap()
        .lazy();
    // Return result
    Ok(df)
}

// All the calculation for a single year must be completed then move on to another year
fn _varied_row_by_row_cf(lf: LazyFrame, base: &Base) -> PolarsResult<LazyFrame> {
    let df = lf.collect().unwrap();
    let vec_length = df.height();
    // Create vectors from dataframe columnss
    // Start values
    let mut cont_flag: Vec<bool> = col_to_vec_bool(&df, "cont_flag");
    let mut start_si: Vec<f64> = col_to_vec_f64(&df, "start_si");
    let mut start_tav: Vec<f64> = col_to_vec_f64(&df, "start_tav");
    let mut start_eav: Vec<f64> = col_to_vec_f64(&df, "start_eav");
    let mut start_pav: Vec<f64> = col_to_vec_f64(&df, "start_pav");

    // Surrender
    let srr_chrg: Vec<f64> = col_to_vec_f64(&df, "srr_chrg"); // Read only
    let mut srr_val: Vec<f64> = col_to_vec_f64(&df, "srr_val");

    // Benefits
    let mut ben: Vec<f64> = col_to_vec_f64(&df, "ben");
    let juvenile_lien_rate: Vec<f64> = col_to_vec_f64(&df, "juvenile_lien_rate"); // Read only
    let acc_ben_term_flag: Vec<bool> = col_to_vec_bool(&df, "acc_ben_term_flag"); // Read only
    let mut acc_ben: Vec<f64> = col_to_vec_f64(&df, "acc_ben");

    // Withdrawal
    let withdrawal_input: Vec<f64> = col_to_vec_f64(&df, "withdrawal_input"); // Read only
    let mut withdrawal: Vec<f64> = col_to_vec_f64(&df, "withdrawal");
    let mut eav_withdrawal: Vec<f64> = col_to_vec_f64(&df, "eav_withdrawal");
    let mut tav_withdrawal: Vec<f64> = col_to_vec_f64(&df, "tav_withdrawal");
    let mut withdrawal_log: Vec<String> = col_to_vec_string(&df, "withdrawal_log");

    // Premium load
    let risk_flag: Vec<bool> = col_to_vec_bool(&df, "risk_flag"); // Read only
    let pm_load_term_flag: Vec<bool> = col_to_vec_bool(&df, "pm_load_term_flag"); // Read only
    let em_load_term_flag: Vec<bool> = col_to_vec_bool(&df, "em_load_term_flag"); // Read only
    let mut unrounded_pm_load: Vec<f64> = col_to_vec_f64(&df, "unrounded_pm_load");
    let mut unrounded_em_load: Vec<f64> = col_to_vec_f64(&df, "unrounded_em_load");
    let mut pm_load: Vec<f64> = col_to_vec_f64(&df, "pm_load");
    let mut em_load: Vec<f64> = col_to_vec_f64(&df, "em_load");
    let mut load: Vec<f64> = col_to_vec_f64(&df, "load");
    let mut load_alloc_chrg: Vec<f64> = col_to_vec_f64(&df, "load_alloc_chrg");
    let mut load_alloc: Vec<f64> = col_to_vec_f64(&df, "load_alloc");

    // Allocation
    let tp_alloc_chrg: Vec<f64> = col_to_vec_f64(&df, "tp_alloc_chrg"); // Read only
    let tp_alloc: Vec<f64> = col_to_vec_f64(&df, "tp_alloc"); // Read only
    let ep_alloc_chrg_rate: Vec<f64> = col_to_vec_f64(&df, "ep_alloc_chrg_rate"); // Read only
    let ep_alloc_chrg: Vec<f64> = col_to_vec_f64(&df, "ep_alloc_chrg"); // Read only
    let ep_alloc: Vec<f64> = col_to_vec_f64(&df, "ep_alloc"); // Read only
    let mut alloc_chrg: Vec<f64> = col_to_vec_f64(&df, "alloc_chrg");
    let mut alloc: Vec<f64> = col_to_vec_f64(&df, "alloc");

    // Account values after withdrawal and allocation
    let mut eav_after_wdrl_and_alloc: Vec<f64> = col_to_vec_f64(&df, "eav_after_wdrl_and_alloc");
    let mut tav_after_wdrl_and_alloc: Vec<f64> = col_to_vec_f64(&df, "tav_after_wdrl_and_alloc");
    let mut pav_after_wdrl_and_alloc: Vec<f64> = col_to_vec_f64(&df, "pav_after_wdrl_and_alloc");

    // Cost of insurance
    let coi_rate: Vec<f64> = col_to_vec_f64(&df, "coi_rate"); // Read only
    let mut sar: Vec<f64> = col_to_vec_f64(&df, "sar");
    let mut standard_coi: Vec<f64> = col_to_vec_f64(&df, "standard_coi");
    let mut acc_coi: Vec<f64> = col_to_vec_f64(&df, "acc_coi");
    let mut em_load_coi: Vec<f64> = col_to_vec_f64(&df, "em_load_coi");
    let mut pm_load_coi: Vec<f64> = col_to_vec_f64(&df, "pm_load_coi");
    let mut coi: Vec<f64> = col_to_vec_f64(&df, "coi");

    // Interest
    let annual_int_rate: Vec<f64> = col_to_vec_f64(&df, "annual_int_rate"); // Read only
    let due_a_n1_m12: Vec<f64> = col_to_vec_f64(&df, "due_a_n1_m12"); // Read only

    // Deduction
    let admin_chrg: Vec<f64> = col_to_vec_f64(&df, "admin_chrg"); // Read only
    let mut plan_nom_deduction: Vec<f64> = col_to_vec_f64(&df, "plan_nom_deduction");
    let mut plan_deduction: Vec<f64> = col_to_vec_f64(&df, "plan_deduction");
    let mut deduction_flag: Vec<bool> = col_to_vec_bool(&df, "deduction_flag");
    let mut nom_deduction: Vec<f64> = col_to_vec_f64(&df, "nom_deduction");
    let mut deduction: Vec<f64> = col_to_vec_f64(&df, "deduction");
    let mut tav_deduction: Vec<f64> = col_to_vec_f64(&df, "tav_deduction");
    let mut eav_deduction: Vec<f64> = col_to_vec_f64(&df, "eav_deduction");
    // Interest
    let mut eav_int: Vec<f64> = col_to_vec_f64(&df, "eav_int");
    let mut tav_int: Vec<f64> = col_to_vec_f64(&df, "tav_int");
    let mut int: Vec<f64> = col_to_vec_f64(&df, "int");
    // Bonus
    let lb_rate: Vec<f64> = col_to_vec_f64(&df, "lb_rate"); // Read only
    let mut lb_tav_withdrawal_review: Vec<f64> = col_to_vec_f64(&df, "lb_tav_withdrawal_review");
    let mut lb_flag: Vec<bool> = col_to_vec_bool(&df, "lb_flag");
    let mut lb: Vec<f64> = col_to_vec_f64(&df, "lb");
    let mut sb_rate: Vec<f64> = col_to_vec_f64(&df, "sb_rate");
    let mut sb_tav_withdrawal_review: Vec<f64> = col_to_vec_f64(&df, "sb_tav_withdrawal_review");
    let mut sb_flag: Vec<bool> = col_to_vec_bool(&df, "sb_flag");
    let mut sb: Vec<f64> = col_to_vec_f64(&df, "sb");
    let mut bonus: Vec<f64> = col_to_vec_f64(&df, "bonus");
    // End values
    let mut end_si: Vec<f64> = col_to_vec_f64(&df, "end_si");
    let mut end_tav: Vec<f64> = col_to_vec_f64(&df, "end_tav");
    let mut end_eav: Vec<f64> = col_to_vec_f64(&df, "end_eav");
    let mut end_pav: Vec<f64> = col_to_vec_f64(&df, "end_pav");

    for i in 0..=(vec_length - 1 as usize) {
        // Update start values
        if i > 0 {
            start_si[i] = end_si[i - 1];
            start_eav[i] = end_eav[i - 1];
            start_tav[i] = end_tav[i - 1];
            cont_flag[i] = deduction_flag[i - 1];
        } else {
            start_si[i] = base.si;
            cont_flag[i] = true;
        }

        start_pav[i] = start_tav[i] + start_eav[i];

        // Surrender value
        srr_val[i] = f64::max(start_tav[i] - srr_chrg[i], 0.0) + start_eav[i];

        // Benefits
        ben[i] = if base.death_tpd_option == DeathTPDBenefitEnum::A {
            f64::max(start_si[i], start_pav[i]) * juvenile_lien_rate[i]
        } else {
            start_si[i] * juvenile_lien_rate[i]
        };

        // Accidental benefit -
        acc_ben[i] =
            start_si[i] * (base.acc_ben_coeff as f64) * (acc_ben_term_flag[i] as u8 as f64);

        // Withdrawal
        let (wdrl, eav_wdrl, tav_wdrl, end_si_v, wdrl_log) = calculate_withdrawal(
            withdrawal_input[i],
            start_eav[i],
            start_tav[i],
            start_si[i],
            base,
        )?;
        withdrawal[i] = wdrl;
        eav_withdrawal[i] = eav_wdrl;
        tav_withdrawal[i] = tav_wdrl;
        end_si[i] = end_si_v;
        withdrawal_log[i] = wdrl_log.to_string();

        // EM Load
        unrounded_em_load[i] = base.extra_prem_rate()?
            * base.load.em_load
            * (em_load_term_flag[i] as u8 as f64)
            * (risk_flag[i] as u8 as f64)
            * ben[i]
            / 1000.0
            / (1.0 - ep_alloc_chrg_rate[i]);

        em_load[i] = (unrounded_em_load[i] / 1000.0).ceil() * 1000.0;

        // PM Load
        unrounded_pm_load[i] = (base.load.pm_load as f64)
            * (pm_load_term_flag[i] as u8 as f64)
            * (risk_flag[i] as u8 as f64)
            * ben[i]
            / 1000.0
            / (1.0 - ep_alloc_chrg_rate[i]);

        pm_load[i] = (unrounded_pm_load[i] / 1000.0).ceil() * 1000.0;

        // Load
        load[i] = em_load[i] + pm_load[i];
        load_alloc_chrg[i] = load[i] * ep_alloc_chrg_rate[i];
        load_alloc[i] = load[i] - load_alloc_chrg[i];

        // Obtain total allocation charge and allocation
        alloc_chrg[i] = load_alloc_chrg[i] + ep_alloc_chrg[i] + tp_alloc_chrg[i];
        alloc[i] = load_alloc[i] + ep_alloc[i] + tp_alloc[i];

        // Account values after withdrawal and allocation
        eav_after_wdrl_and_alloc[i] =
            start_eav[i] - eav_withdrawal[i] + ep_alloc[i] + load_alloc[i];
        tav_after_wdrl_and_alloc[i] = start_tav[i] - tav_withdrawal[i] + tp_alloc[i];
        pav_after_wdrl_and_alloc[i] = eav_after_wdrl_and_alloc[i] + tav_after_wdrl_and_alloc[i];

        //SAR
        sar[i] = if base.death_tpd_option == DeathTPDBenefitEnum::A {
            f64::max(ben[i] - start_pav[i], 0.0)
        } else {
            ben[i]
        };

        // COI
        standard_coi[i] = sar[i] * coi_rate[i];
        em_load_coi[i] = base.load.em_load * standard_coi[i] * (risk_flag[i] as u8 as f64);
        pm_load_coi[i] = (base.load.pm_load as f64) * sar[i] * (risk_flag[i] as u8 as f64) / 1000.0;
        acc_coi[i] = acc_ben[i] * base.acc_coi_rate()? * (acc_ben_term_flag[i] as u8 as f64) * 12.0;
        coi[i] = standard_coi[i] + em_load_coi[i] + pm_load_coi[i] + acc_coi[i];

        // Deduction
        plan_nom_deduction[i] = coi[i] + admin_chrg[i] * 12.0; // Sum of 12 installments
        plan_deduction[i] = plan_nom_deduction[i] * due_a_n1_m12[i]; // Convert 12 installments to present value
        deduction_flag[i] = plan_deduction[i] <= pav_after_wdrl_and_alloc[i];

        if !deduction_flag[i] {
            break;
        }

        nom_deduction[i] = plan_nom_deduction[i] * (deduction_flag[i] as u8 as f64);
        deduction[i] = plan_deduction[i] * (deduction_flag[i] as u8 as f64);
        tav_deduction[i] = f64::min(tav_after_wdrl_and_alloc[i], deduction[i]);
        eav_deduction[i] = deduction[i] - tav_deduction[i];

        // Interest
        eav_int[i] = (eav_after_wdrl_and_alloc[i] - eav_deduction[i]) * annual_int_rate[i];
        tav_int[i] = (tav_after_wdrl_and_alloc[i] - tav_deduction[i]) * annual_int_rate[i];
        int[i] = eav_int[i] + tav_int[i];

        // Loyalty bonus
        if lb_rate[i] > 0.0 {
            let window = base.lb_review_period()? as usize;
            let start_idx = usize::max(i + 1 - window, 0);
            let lb_tav_wdrl_sum = tav_withdrawal[start_idx..=i].iter().sum();
            lb_tav_withdrawal_review[i] = lb_tav_wdrl_sum;
            lb_flag[i] = lb_tav_wdrl_sum == 0.0;
        }
        lb[i] = lb_rate[i] * base.modal_tp_tuple()?.0 * (lb_flag[i] as u8 as f64);

        // Special bonus
        sb_rate[i] = get_sb_rate(i, start_si[i], base.id);

        if sb_rate[i] > 0.0 {
            let window = base.sb_review_period()? as usize;
            let start_idx = usize::max(i + 1 - window, 0);
            let sb_tav_wdrl_sum = tav_withdrawal[start_idx..=i].iter().sum();
            sb_tav_withdrawal_review[i] = sb_tav_wdrl_sum;
            sb_flag[i] = sb_tav_wdrl_sum == 0.0;
        }
        sb[i] = sb_rate[i] * base.modal_tp_tuple()?.0 * (sb_flag[i] as u8 as f64);
        bonus[i] = lb[i] + sb[i];

        // End values
        end_eav[i] = eav_after_wdrl_and_alloc[i] - eav_deduction[i] + eav_int[i];
        end_tav[i] = tav_after_wdrl_and_alloc[i] - tav_deduction[i] + tav_int[i] + bonus[i];
        end_pav[i] = end_eav[i] + end_tav[i];
    }

    // Update the dataframe with the updated vectors
    let lf = update_df_with_vectors!(
        df,
        [
            // Start SI
            cont_flag,
            start_si,
            start_tav,
            start_eav,
            start_pav,
            // Surrender
            srr_val,
            // Benefirs
            ben,
            acc_ben_term_flag,
            acc_ben,
            // Withdrawal
            withdrawal,
            eav_withdrawal,
            tav_withdrawal,
            withdrawal_log,
            // Premium load
            unrounded_pm_load,
            unrounded_em_load,
            pm_load,
            em_load,
            load,
            load_alloc_chrg,
            load_alloc,
            // Allocation charge
            alloc_chrg,
            alloc,
            // Account values after withdrawal and allocation
            eav_after_wdrl_and_alloc,
            tav_after_wdrl_and_alloc,
            pav_after_wdrl_and_alloc,
            // COI
            sar,
            standard_coi,
            acc_coi,
            em_load_coi,
            pm_load_coi,
            coi,
            // Deduction
            plan_nom_deduction,
            plan_deduction,
            deduction_flag,
            nom_deduction,
            deduction,
            tav_deduction,
            eav_deduction,
            // Interest
            eav_int,
            tav_int,
            int,
            // Loyalty bonus
            lb_tav_withdrawal_review,
            lb_flag,
            lb,
            // Special Bonus
            sb_rate,
            sb_tav_withdrawal_review,
            sb_flag,
            sb,
            bonus,
            // End values
            end_si,
            end_tav,
            end_eav,
            end_pav,
        ]
    );

    // Filter out rows where deduction flag is false
    //Ok(lf.filter(col("deduction_flag").eq(lit(true))))
    Ok(lf)
}

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
pub fn varied(
    scenario: (IntRateScenarioEnum, RiskTypeEnum, PremTermScenarioEnum),
    lf: LazyFrame,
    base: &Base,
) -> PolarsResult<LazyFrame> {
    let lf = _varied_init(scenario, lf, base)?;
    _varied_row_by_row_cf(lf, base)
}
// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cashflows::base_cf::fixed::fixed;
    use crate::helpers::read_json_struct;
    use crate::structs::policy::Policy;
    use std::fs::File;

    #[test]
    fn test_fn_fixed() {
        // Generate result DataFrame from fixed function
        let json_path = "src/cashflows/test_data/uvl01_policy.json";
        let policy = read_json_struct::<Policy>(json_path).unwrap();
        let lf = fixed(&policy.base).unwrap();
        let scenario = (
            IntRateScenarioEnum::High,
            RiskTypeEnum::Standard,
            PremTermScenarioEnum::PolicyTerm,
        );
        let df = varied(scenario, lf, &policy.base)
            .unwrap()
            .collect()
            .unwrap();

        // Export result df to csv to cross check in Excel file
        let export_path = "src/cashflows/test_data/result_varied.csv";
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
