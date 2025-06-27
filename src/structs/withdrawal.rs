use super::*;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Withdrawal {
    #[garde(range(min = 0))]
    pub from: i32, // policy year

    #[garde(range(min = 0))]
    pub to: i32, // policy year

    #[garde(range(min = 0.0))]
    pub amount: f64,
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {}
