use super::*;
use crate::structs::{base::Base, people::Owner, rider::Rider};

//-----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
fn base_validation(value: &Base, _ctx: &()) -> garde::Result {
    _term_validation(value, _ctx)?;
    _withdrawal_start_year_validation(value, _ctx)?;
    _fund_alloc_default_validation(value, _ctx)?;
    Ok(())
}

fn _term_validation(value: &Base, _ctx: &()) -> garde::Result {
    // Map error to garde::Error
    let term = match value.term() {
        Ok(t) => t,
        Err(e) => {
            let err_msg = format!("Failed to get base/policy term: {}", e);
            return Err(garde::Error::new(err_msg));
        }
    };

    let must_pay_period = match value.must_pay_period() {
        Ok(mpp) => mpp,
        Err(e) => {
            let err_msg = format!("Failed to get must pay period: {}", e);
            return Err(garde::Error::new(err_msg));
        }
    };

    // Opted tp term cannot exceed base/policy term
    if value.opted_tp_term > term {
        let err_msg = format!(
            "Opted term {} cannot be greater than base/policy term {}.",
            value.opted_tp_term, term
        );
        return Err(garde::Error::new(err_msg));
    }

    // opted_ep_term cannot exceed opted_ep_term
    if value.opted_ep_term > value.opted_tp_term {
        let err_msg = format!(
            "Opted EP term {} cannot be greater than opted TP term {}.",
            value.opted_ep_term, value.opted_tp_term
        );
        return Err(garde::Error::new(err_msg));
    }

    // opted_ep_term must be at least must pay period
    if value.opted_ep_term < must_pay_period {
        let err_msg = format!(
            "Opted EP term {} cannot be less than must pay period {}.",
            value.opted_ep_term, must_pay_period
        );
        return Err(garde::Error::new(err_msg));
    }

    // Load term validation - this is base on base input
    let load = &value.load;

    // Validate em_load_term
    if load.em_load_term > term {
        let err_msg = format!(
            "EM load term {} cannot be greater than base/policy term {}.",
            load.em_load_term, term
        );
        return Err(garde::Error::new(err_msg));
    }

    // Validate pm_load_term
    if load.pm_load_term > term {
        let err_msg = format!(
            "PM load term {} cannot be greater than base/policy term {}.",
            load.pm_load_term, term
        );
        return Err(garde::Error::new(err_msg));
    }

    Ok(())
}

fn _withdrawal_start_year_validation(value: &Base, _ctx: &()) -> garde::Result {
    // Withdrawal start year validation - this is base on base input
    if let Some(plan) = &value.withdrawal_plan {
        if let Some(w) = plan.first() {
            let start_year = value.withdrawal_start_year().unwrap_or(0);

            if w.from < start_year {
                let err_msg = format!(
                    "Withdrawal must start from the {}. Current withdrawal starts from {}.",
                    start_year, w.from
                );
                return Err(garde::Error::new(err_msg));
            }
        }
    }
    Ok(())
}

fn _fund_alloc_default_validation(value: &Base, _ctx: &()) -> garde::Result {
    // Fund allocation validation - this is base on base input
    let fa = &value.fund_alloc;
    match value.id {
        ULEnum::UVL01 | ULEnum::UVL02 | ULEnum::UVL03 => {
            let err_msg = "Fund allocation must contain exactly one entry: \"F000\".";
            if fa.len() != 1 || fa[0].fund != FundEnum::F000 {
                return Err(garde::Error::new(err_msg));
            }
        }
        _ => {
            // For other plans, fund allocation can be empty or contain multiple entries
        }
    }
    Ok(())
}

//-----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Policy {
    pub id: String,

    #[garde(dive)]
    pub owner: Owner,

    pub created_date: NaiveDate,

    #[garde(custom(base_validation))]
    #[garde(dive)]
    pub base: Base,

    #[garde(dive)]
    pub rider: Option<Vec<Rider>>,
}

//-----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::read_json_struct;
    use crate::structs::policy::Policy;

    #[test]
    fn test_struct_policy_validation_01() {
        // Load a valid policy from JSON and validate it
        let path = "src/models/test_data/uvl01_policy.json";
        let policy: Policy = read_json_struct(path).expect("Failed to read policy JSON");
        policy.validate().expect("Policy validation failed");
    }
}
