use garde::Validate;
use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------------
// PRIVATE
// -----------------------------------------------------------------------------
fn em_load_validation(value: &f64, _ctx: &()) -> garde::Result {
    // Check if the value is a multiple of 0.25
    if value % 0.25 != 0.0 {
        return Err(garde::Error::new("Value is not a multiple of 0.25"));
    }
    Ok(())
}
// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct Load {
    #[garde(custom(em_load_validation))]
    #[garde(range(min = 0.0, max = 2.5))]
    pub em_load: f64,

    #[garde(range(min = 0))]
    pub em_load_term: i32, // years

    #[garde(range(min = 0, max = 15))]
    pub pm_load: i32,

    #[garde(range(min = 0))]
    pub pm_load_term: i32, // years
}

// -----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::from_str;

    // Test struct Load
    #[test]
    fn test_struct_load_01() {
        // Basic test for em_load - valid value
        let json = r#"{
        "em_load": 0.75,
        "em_load_term": 20,
        "pm_load": 2,
        "pm_load_term": 20
        }"#;
        let load: Load = from_str(&json).unwrap();
        assert!(load.validate().is_ok());
        assert_eq!(load.em_load, 0.75);
    }

    #[test]
    #[should_panic]
    fn test_struct_load_02() {
        // Test for em_load > 2.5
        let json = r#"{
        "em_load": 3.5,
        "em_load_term": 20,
        "pm_load": 2,
        "pm_load_term": 20
        }"#;
        let load: Load = from_str(&json).unwrap();
        assert!(load.validate().is_ok());
    }

    #[test]
    #[should_panic]
    fn test_struct_load_03() {
        // Test for em_load not multiple of 0.25
        let json = r#"{
        "em_load": 2.15,
        "em_load_term": 20,
        "pm_load": 2,
        "pm_load_term": 20
        }"#;
        let load: Load = from_str(&json).unwrap();
        assert!(load.validate().is_ok());
    }

    #[test]
    #[should_panic]
    fn test_struct_load_04() {
        // Test for pm_load > 15
        let json = r#"{
        "em_load": 2.5,
        "em_load_term": 20,
        "pm_load": 18,
        "pm_load_term": 20
        }"#;
        let load: Load = from_str(&json).unwrap();
        assert!(load.validate().is_ok());
    }

    // // Test struct withdrawal plan
    // #[test]
    // fn test_struct_base_01() {
    //     let base: Base =
    //         read_base_json("/home/learndev/proj/sirl/src/models/test_data/base.json").unwrap();
    //     assert!(base.validate().is_ok());
    // }
}
