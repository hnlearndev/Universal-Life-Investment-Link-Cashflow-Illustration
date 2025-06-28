use polars::prelude::*;
use serde_json::from_reader;
use std::fs::File;
use std::io::BufReader;

// -----------------------------------------------------------------------------
// PUBLIC
// -----------------------------------------------------------------------------
/// Reads a JSON file and deserializes it into a struct of type T.
pub fn read_json_struct<T>(path: &str) -> Result<T, serde_json::Error>
where
    T: serde::de::DeserializeOwned,
{
    let file = File::open(path).map_err(serde_json::Error::io)?;
    let reader = BufReader::new(file);
    let data: T = from_reader(reader)?;
    Ok(data)
}

// Compares two objects that can be converted to a DataFrame (DataFrame or LazyFrame).
pub fn compare_frames<F>(frame1: &F, frame2: &F) -> PolarsResult<bool>
where
    F: AsFrame,
{
    let df1 = frame1.as_frame()?;
    let df2 = frame2.as_frame()?;

    // Compare shape and columns names first first
    if df1.shape() != df2.shape() || df1.get_column_names() != df2.get_column_names() {
        return Ok(false);
    }

    for (s1, s2) in df1.get_columns().iter().zip(df2.get_columns()) {
        match (s1.dtype(), s2.dtype()) {
            (DataType::Float64, DataType::Float64) => {
                let a = s1.f64()?;
                let b = s2.f64()?;
                for (va, vb) in a.into_iter().zip(b) {
                    match (va, vb) {
                        (Some(x), Some(y)) if (x - y).abs() > f64::EPSILON => return Ok(false),
                        (Some(_), None) | (None, Some(_)) => return Ok(false),
                        _ => {}
                    }
                }
            }
            // Add a catch-all arm for all other types
            _ => {
                if s1 != s2 {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}
/// Trait to convert DataFrame or LazyFrame to DataFrame for comparison.
pub trait AsFrame {
    fn as_frame(&self) -> PolarsResult<DataFrame>;
}

impl AsFrame for DataFrame {
    fn as_frame(&self) -> PolarsResult<DataFrame> {
        Ok(self.clone())
    }
}

impl AsFrame for LazyFrame {
    fn as_frame(&self) -> PolarsResult<DataFrame> {
        self.clone().collect()
    }
}

//-----------------------------------------------------------------------------
// UNIT TESTS
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::policy::Policy;

    #[test]
    fn test_fn_read_policy_json_01() {
        // Path to a valid policy.json file
        let path = "src/models/test_data/uvl01_policy.json";
        let result = read_json_struct::<Policy>(path);
        assert!(result.is_ok(), "Expected Ok(Policy), got {:?}", result);
    }

    #[test]
    fn test_fn_read_policy_json_02() {
        // Path to a non-existent file
        let path = "/nonexistent/path/policy.json";
        let result = read_json_struct::<Policy>(path);
        assert!(result.is_err(), "Expected Err, got {:?}", result);
    }
}
