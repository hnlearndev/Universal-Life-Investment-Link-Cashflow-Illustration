use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Owner {
    pub id: String,

    #[garde(length(min = 12, max = 12))]
    pub ssn: String,

    pub dob: NaiveDate,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Agent {
    pub id: String,

    #[garde(length(min = 12, max = 12))]
    pub ssn: String,

    pub dob: NaiveDate,

    pub gender: GenderEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Beneficiary {
    pub id: String,

    #[garde(length(min = 12, max = 12))]
    pub ssn: String,

    pub dob: NaiveDate,
    pub relatsh: RelationshipToOwnerEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Insured {
    pub id: String,

    #[garde(length(min = 12, max = 12))]
    pub ssn: String,

    pub dob: NaiveDate,
    pub gender: GenderEnum,
    pub relatsh: RelationshipToOwnerEnum,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::from_str;

    #[test]
    fn test_struct_owner() {
        let json = r#"{
            "id": "owner123",
            "ssn": "123456789012",
            "dob": "1980-01-01"
        }"#;
        let owner: Owner = from_str(&json).unwrap();
        assert!(owner.validate().is_ok());
    }

    // Test struct Insured
    #[test]
    fn test_struct_insured_01() {
        let json = r#"{
        "id": "0000000000000000001",
        "ssn": "012345678999",
        "dob": "1988-11-17",
        "gender": "Female",
        "relatsh": "OwnerSelf"
        }"#;
        let insured: Insured = serde_json::from_str(&json).unwrap();
        assert!(insured.validate().is_ok());
    }
}
