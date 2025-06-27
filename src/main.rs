mod cashflows;
mod database;
mod enums;
mod helpers;
mod structs;

use cashflows::base_cf::base_cf;
use helpers::read_json_struct;
use structs::policy::Policy;

fn main() {
    // Demo with src/cashflows/test_data contains details testing
    let json_path = "src/cashflows/test_data/uvl01_policy.json";
    let policy = read_json_struct::<Policy>(json_path).unwrap();
    let df = base_cf(&policy.base).unwrap().collect().unwrap();
    println!("{:?}", df);
}
