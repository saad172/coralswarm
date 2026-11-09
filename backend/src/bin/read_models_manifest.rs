fn main() {
    println!(
        "{}",
        serde_json::to_string_pretty(&coralswarm_backend::read_models::registry::manifest())
            .unwrap()
    );
}
