// use crate::enums::{GenderEnum, IntRateScenarioEnum, ULEnum};
use crate::enums::{GenderEnum, IntRateScenarioEnum, ULEnum};
use polars::prelude::*;
use strum_macros::{AsRefStr, EnumString};

pub mod admin_chrg;
pub mod age_validation;
pub mod alloc_chrg_rate;
pub mod coi_rate;
pub mod extra_prem_rate;
pub mod int_rate;
pub mod juvenile_lien_rate;
pub mod lb_rate;
pub mod modal_factor;
pub mod prem_rate;
pub mod srr_chrg_rate;
