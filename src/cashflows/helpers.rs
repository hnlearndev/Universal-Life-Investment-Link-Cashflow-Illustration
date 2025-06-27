use crate::enums::{DeathTPDBenefitEnum, ULEnum};
use crate::structs::base::Base;
use polars::prelude::*;

#[macro_export]
macro_rules! update_df_with_vectors {
    ($df:expr, [ $( $vec:ident ),* $(,)? ]) => {{
        let mut lf = $df.clone().lazy();
        $(
            let name = stringify!($vec);
            let expr = lit(Series::new(name.into(), $vec.clone())).alias(name);
            lf = lf.with_column(expr);
        )*
        lf
    }};
}

// -----------------------------------------------------------------------------
// Below functions are used selectively with pre-defined column names of known types.
// Hence we can safely assume the column types and panic if they do not match.
pub fn col_to_vec_f64(df: &DataFrame, col_name: &str) -> Vec<f64> {
    let col = df.column(col_name).expect("Column not found");
    _assert_column_dtype(col, &DataType::Float64);
    col.f64()
        .expect("Type mismatch")
        .into_iter()
        .map(|v| v.unwrap_or(0.0))
        .collect()
}

pub fn col_to_vec_i32(df: &polars::prelude::DataFrame, col_name: &str) -> Vec<i32> {
    let col = df.column(col_name).expect("Column not found");
    _assert_column_dtype(col, &DataType::Boolean);
    col.i32()
        .expect("Type mismatch")
        .into_iter()
        .map(|v| v.unwrap_or(0))
        .collect()
}

pub fn col_to_vec_bool(df: &DataFrame, col_name: &str) -> Vec<bool> {
    let col = df.column(col_name).expect("Column not found");
    _assert_column_dtype(col, &DataType::Boolean);
    col.bool()
        .expect("Type mismatch")
        .into_iter()
        .map(|v| v.unwrap_or(false))
        .collect()
}

pub fn col_to_vec_string(df: &DataFrame, col_name: &str) -> Vec<String> {
    let col = df.column(col_name).expect("Column not found");
    _assert_column_dtype(col, &DataType::String);
    col.str()
        .expect("Type mismatch")
        .into_iter()
        .map(|v| v.unwrap_or("").to_string())
        .collect()
}

fn _assert_column_dtype(col: &Column, expected_dtype: &DataType) {
    let dtype = col.dtype();
    if dtype != expected_dtype {
        panic!(
            "Column '{}' is not of type {:?}, but {:?}",
            col.name(),
            expected_dtype,
            dtype
        );
    }
}

// -----------------------------------------------------------------------------
pub fn calculate_withdrawal(
    amount: f64,
    eav: f64,
    tav: f64,
    si: f64,
    base: &Base,
) -> PolarsResult<(f64, f64, f64, f64, &str)> {
    // No withdrawal
    if amount == 0.0 {
        return Ok((0.0, 0.0, 0.0, si, "No withdrawal."));
    }

    // When withdrawal amount exceeds current policy account value
    let pav = tav + eav;
    if amount > pav {
        return Ok((
            0.0,
            0.0,
            0.0,
            si,
            "Withdrawal amount exceeds policy account value.",
        ));
    }
    // When policy account value after withdrawal is lower than acceptable limit
    let pav_withdrawal = amount;
    let eav_withdrawal = eav.min(amount);
    let tav_withdrawal = amount - eav_withdrawal;

    let end_eav = eav - eav_withdrawal;
    let end_tav = tav - tav_withdrawal;
    let end_pav = end_eav + end_tav;

    if end_pav < base.min_pav_after_withdrawal()? {
        return Ok((
            0.0,
            0.0,
            0.0,
            si,
            "After withdrawal, end PAV is below acceptable limit.",
        ));
    }

    // When sum insured after withdrawal is lower than acceptable limit
    let end_si = if base.death_tpd_option == DeathTPDBenefitEnum::A {
        si
    } else {
        f64::max(pav, si) - amount
    };

    if end_si < base.min_si()? {
        return Ok((
            0.0,
            0.0,
            0.0,
            si,
            "After withdrawal, end SI is below acceptable limit.",
        ));
    }

    Ok((
        pav_withdrawal,
        eav_withdrawal,
        tav_withdrawal,
        end_si,
        "Successfully withdraw.",
    ))
}

pub fn get_sb_rate(year: usize, si: f64, product: ULEnum) -> f64 {
    match product {
        ULEnum::UVL02 | ULEnum::UVL03 => match (year, si) {
            (10, si) if si >= 1_000_000_000.0 => 0.4,
            (20, si) if si >= 1_000_000_000.0 => 1.2,
            (10, si) if si >= 500_000_000.0 => 0.2,
            (20, si) if si >= 500_000_000.0 => 0.6,
            (10, _) => 0.2,
            (20, _) => 0.6,
            _ => 0.0,
        },
        _ => 0.0,
    }
}
