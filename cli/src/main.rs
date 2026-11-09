use clap::Parser;

#[derive(Parser)]
#[command(name = "cs", version, about = "Coralswarm CLI")]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
}
