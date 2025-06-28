use super::*;
use crate::enums::RiderEnum;
use crate::structs::{load::Load, people::Insured};

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Rider {
    pub id: RiderEnum,
    pub rcd: NaiveDate,
    pub paymode: PayModeEnum,
    pub channel: ChannelEnum,
    pub status: StatusEnum,
    pub insured: Insured,
    pub load: Option<Load>,
    pub si: Option<f64>,
    pub hop2_option: Option<i32>,
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {}
