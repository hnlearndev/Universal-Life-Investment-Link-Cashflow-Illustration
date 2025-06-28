use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct FundAlloc {
    pub fund: FundEnum,

    #[garde(range(min = 0, max = 100))]
    pub tp_pct: i32, // whole number then /100

    #[garde(range(min = 0, max = 100))]
    pub ep_pct: i32, // whole number then /100
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
