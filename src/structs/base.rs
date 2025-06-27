use super::*;

use crate::database::{
    admin_chrg::get_admin_chrg_lf,
    alloc_chrg_rate::{get_ep_alloc_chrg_rate_lf, get_tp_alloc_chrg_rate_lf},
    coi_rate::get_coi_rate_lf,
    extra_prem_rate::get_extra_prem_rate,
    int_rate::{get_gir, get_hir, get_lir},
    juvenile_lien_rate::get_juvenile_lien_rate_lf,
    lb_rate::get_lb_rate_lf,
    modal_factor::get_modal_factor_tuple,
    prem_rate::get_prem_rate,
    srr_chrg_rate::get_srr_chrg_rate_lf,
};
use crate::structs::{
    fund_alloc::FundAlloc,
    helpers::{calculate_age, calculate_month_age},
    load::Load,
    people::Insured,
    withdrawal::Withdrawal,
};

// -----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
fn fund_alloc_sum_pct_validation(value: &Vec<FundAlloc>, _ctx: &()) -> garde::Result {
    // Cannot be empty vec![]
    if value.is_empty() {
        return Err(garde::Error::new("Fund allocation cannot be empty"));
    }

    // Sum of all fund allocations must equal 100%
    let total_tp_pct: f64 = value.iter().map(|fa| fa.tp_pct as f64).sum();
    let total_ep_pct: f64 = value.iter().map(|fa| fa.ep_pct as f64).sum();

    if (total_tp_pct - 100.0).abs() > f64::EPSILON {
        return Err(garde::Error::new(
            "Total TP fund allocation must equal 100%",
        ));
    }

    if (total_ep_pct - 100.0).abs() > f64::EPSILON {
        return Err(garde::Error::new(
            "Total EP fund allocation must equal 100%",
        ));
    }

    Ok(())
}

fn withdrawal_plan_year_input_validation(
    value: &Option<Vec<Withdrawal>>,
    _ctx: &(),
) -> garde::Result {
    if value.is_none() {
        return Ok(());
    }

    for (i, row) in value.iter().flatten().enumerate() {
        // 'from' cannot be less than 'to'
        if row.from > row.to {
            let err_msg = format!(
                "Withdrawal 'from' year ({}) cannot be greater than 'to' year ({}).",
                row.from, row.to
            );
            return Err(garde::Error::new(err_msg));
        }

        // "from" value must be greater than previous "to" value
        if i > 0 {
            let prev_row = &value.as_ref().unwrap()[i - 1];
            if row.from <= prev_row.to {
                let err_msg = format!(
                    "Withdrawal 'from' year ({}) must be greater than previous 'to' year ({}).",
                    row.from, prev_row.to
                );
                return Err(garde::Error::new(err_msg));
            }
        }
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Base {
    pub id: ULEnum,
    pub rcd: NaiveDate,
    pub paymode: PayModeEnum,
    pub channel: ChannelEnum,
    pub status: StatusEnum,

    #[garde(dive)]
    pub insured: Insured,

    #[garde(dive)]
    pub load: Load,

    #[garde(range(min = 0.0))]
    pub si: f64,

    #[garde(range(min = 0))]
    pub opted_tp_term: i32,

    #[garde(range(min = 0.0))]
    pub ep: f64,

    #[garde(range(min = 0))]
    pub opted_ep_term: i32,

    pub death_tpd_option: DeathTPDBenefitEnum,

    #[garde(range(min = 1, max = 2))]
    pub maturity_option: i32,

    #[garde(range(min = 1, max = 5))]
    pub acc_ben_coeff: i32,

    #[garde(custom(fund_alloc_sum_pct_validation))]
    #[garde(dive)]
    pub fund_alloc: Vec<FundAlloc>,

    #[garde(custom(withdrawal_plan_year_input_validation))]
    #[garde(dive)]
    pub withdrawal_plan: Option<Vec<Withdrawal>>,
}

impl Base {
    // -------------------------------------------------
    // The values that are from input
    // -------------------------------------------------
    // Most values are returned as PolarsResult(LazyFrame), not Result<PolarsResult(LazyFrame)>, because validation occurs before any calculation.
    // This ensures that by the time these methods are called, inputs are already validated,
    // so error handling via Result is unnecessary here and simplifies downstream code.
    pub fn entry_age(&self) -> PolarsResult<i32> {
        calculate_age(&self.insured.dob, &self.rcd).map_err(|e| PolarsError::ComputeError(e.into()))
    }

    pub fn entry_month_age(&self) -> PolarsResult<i32> {
        if self.entry_age()? == 0 {
            calculate_month_age(&self.insured.dob, &self.rcd)
                .map_err(|e| polars::prelude::PolarsError::ComputeError(e.into()))
        } else {
            // Indicate value is unusable
            Err(PolarsError::ComputeError("Not applicable".into()))
        }
    }

    pub fn maturity_age(&self) -> PolarsResult<i32> {
        let err = Err(PolarsError::ComputeError("Unsupported product ID".into()));

        match self.maturity_option {
            1 => match &self.id {
                ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(100),
                _ => err,
            },
            _ => match &self.id {
                ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(66),
                _ => err,
            },
        }
    }

    pub fn term(&self) -> PolarsResult<i32> {
        // The two methos already return PolarsResult<i32>, so we can use the ? operator directly
        Ok(&self.maturity_age()? - &self.entry_age()?)
    }

    pub fn acc_ben_term(&self) -> PolarsResult<i32> {
        match &self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => {
                Ok(66 - self.entry_age()?)
            }
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn withdrawal_plan_lf(&self) -> PolarsResult<LazyFrame> {
        let mut years = Vec::new();
        let mut amounts = Vec::new();

        if let Some(withdrawal_plan) = &self.withdrawal_plan {
            for withdrawal in withdrawal_plan.iter() {
                for year in withdrawal.from..=withdrawal.to {
                    years.push(year as i32);
                    amounts.push(withdrawal.amount as f64);
                }
            }
        }

        let df = df![
        "year" => years,
        "withdrawal_input" => amounts
        ]
        .expect("Failed to create DataFrame in withdrawal_lf");

        Ok(df.lazy())
    }
    // -------------------------------------------------
    // The values that are set from database
    // -------------------------------------------------
    pub fn extra_prem_rate(&self) -> PolarsResult<f64> {
        let term = &self.load.em_load_term - 1;
        get_extra_prem_rate(&self.id, &self.insured.gender, &self.entry_age()?, &term)
    }

    pub fn prem_rate(&self) -> PolarsResult<f64> {
        get_prem_rate(&self.id, &self.insured.gender, &self.entry_age()?)
    }

    pub fn modal_factor_tuple(&self) -> PolarsResult<(f64, f64, f64, f64)> {
        get_modal_factor_tuple(&self.id)
    }

    pub fn modal_tp_tuple(&self) -> PolarsResult<(f64, f64, f64, f64)> {
        let (f1, f2, f3, f4) = &self.modal_factor_tuple()?;
        let crude_prem = self.prem_rate()? * self.si / 1000.0;
        let result = (
            (f1 * crude_prem / 1000.0).ceil() * 1000.0,
            (f2 * crude_prem / 1000.0).ceil() * 1000.0,
            (f3 * crude_prem / 1000.0).ceil() * 1000.0,
            (f4 * crude_prem / 1000.0).ceil() * 1000.0,
        );
        Ok(result)
    }

    pub fn modal_ep_tuple(&self) -> PolarsResult<(f64, f64, f64, f64)> {
        let (f1, f2, f3, f4) = &self.modal_factor_tuple()?;
        let result = (
            (f1 * &self.ep / 1000.0).ceil() * 1000.0,
            (f2 * &self.ep / 1000.0).ceil() * 1000.0,
            (f3 * &self.ep / 1000.0).ceil() * 1000.0,
            (f4 * &self.ep / 1000.0).ceil() * 1000.0,
        );
        Ok(result)
    }

    pub fn tp_alloc_chrg_rate_lf(&self) -> PolarsResult<LazyFrame> {
        get_tp_alloc_chrg_rate_lf(&self.id)
    }

    pub fn ep_alloc_chrg_rate_lf(&self) -> PolarsResult<LazyFrame> {
        get_ep_alloc_chrg_rate_lf(&self.id)
    }

    pub fn srr_chrg_rate_lf(&self) -> PolarsResult<LazyFrame> {
        get_srr_chrg_rate_lf(&self.id)
    }

    pub fn juvenile_lien_rate_lf(&self) -> PolarsResult<LazyFrame> {
        get_juvenile_lien_rate_lf(&self.id)
    }

    pub fn int_rate_tuple(&self) -> PolarsResult<(f64, f64, f64)> {
        let hir = get_hir(&self.id)?;
        let lir = get_lir(&self.id)?;
        let gir = match &self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 => get_gir(&self.id)?,
            _ => 0_f64,
        };
        Ok((hir, lir, gir))
    }

    pub fn admin_chrg_lf(&self) -> PolarsResult<LazyFrame> {
        get_admin_chrg_lf(&self.id)
    }

    pub fn lb_rate_lf(&self) -> PolarsResult<LazyFrame> {
        get_lb_rate_lf(&self.id)
    }

    pub fn coi_rate_lf(&self) -> PolarsResult<LazyFrame> {
        get_coi_rate_lf(&self.id, &self.insured.gender)
    }

    // -------------------------------------------------
    // The values that are set manually not from database
    // -------------------------------------------------
    pub fn big_case_bonus(&self) -> PolarsResult<f64> {
        let (tp, _, _, _) = self.modal_tp_tuple()?;

        match self.id {
            ULEnum::ILP01 if tp >= 100_000_000.0 => Ok(tp * 0.05),
            ULEnum::ILP01 if tp >= 50_000_000.0 => Ok(tp * 0.03),
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 => Ok(0.0),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn must_pay_period(&self) -> PolarsResult<i32> {
        match self.id {
            ULEnum::UVL01 => Ok(4),
            ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(3),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn min_pav_after_withdrawal(&self) -> PolarsResult<f64> {
        let (tp, _, _, _) = self.modal_tp_tuple()?;

        match self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(tp),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn min_si(&self) -> PolarsResult<f64> {
        match self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(100_000_000.0),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn withdrawal_start_year(&self) -> PolarsResult<i32> {
        match self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(2),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn acc_coi_rate(&self) -> PolarsResult<f64> {
        // Monthly
        match self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 => Ok(0.000075),
            ULEnum::ILP01 => Ok(0.0),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn lb_review_period(&self) -> PolarsResult<i32> {
        match self.id {
            ULEnum::UVL01 => Ok(4),
            ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(3),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }

    pub fn sb_review_period(&self) -> PolarsResult<i32> {
        match self.id {
            ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 | ULEnum::ILP01 => Ok(10),
            _ => Err(PolarsError::ComputeError("Unsupported product ID".into())),
        }
    }
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
mod tests {
    use super::*;
    use crate::helpers::read_json_struct;
    use crate::structs::policy::Policy;

    #[test]
    fn test_fn_base_struct_01() {
        // Generate result DataFrame from fixed function
        let json_path = "src/cashflows/test_data/uvl01_policy.json";
        let policy = read_json_struct::<Policy>(json_path).unwrap();
        // Method modal_tp_tuple()
        assert_eq!(
            policy.base.modal_tp_tuple().unwrap().0,
            1_072_000.0,
            "Failed to get modal TP tuple"
        );
    }
}
