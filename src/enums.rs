use garde::Validate;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumIter, EnumString};

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum GenderEnum {
    Unknown,
    Male,
    Female,
    NotApplicable = 9,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum PayModeEnum {
    Annual,
    SemiAnnual,
    Quarterly,
    Monthly,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum ChannelEnum {
    CHNL001,
    CHNL002,
    CHNL003,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum StatusEnum {
    Inforce,
    Lapsed,
    Terminated,
    Claimed,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum DeathTPDBenefitEnum {
    A,
    B,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum RelationshipToOwnerEnum {
    OwnerSelf,
    Other,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum FundEnum {
    F000,
    F001,
    F002,
    F003,
    F004,
    F005,
    F006,
    F007,
    F008,
    F009,
    F010,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum IntRateScenarioEnum {
    High,
    Low,
    Guaranteed,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum PremTermScenarioEnum {
    PolicyTerm,
    OptedTerm,
    MustPayTerm,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum RiskTypeEnum {
    Standard,
    Subrisk,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum ULEnum {
    UVL01,
    UVL02,
    UVL03,
    ILP01,
    ILP02,
    ILP03,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum BaseEnum {
    UVL01,
    UVL02,
    UVL03,
    ILP01,
    ILP02,
    ILP03,
}

#[derive(
    Copy, AsRefStr, Debug, PartialEq, EnumString, Clone, Serialize, Deserialize, Validate, EnumIter,
)]
#[garde(allow_unvalidated)]
pub enum RiderEnum {
    ADD01,
    PPD01,
    CIR01,
    CIR02,
    SUP01,
    HOP02,
    WOP01,
    WOP02,
}
