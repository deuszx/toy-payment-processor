use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

const HEADER: &str = "client,available,held,total,locked\n";

/// Builds a run of the binary on `input`, written to a per-test file named `name`.
fn command_on(name: &str, input: &str) -> Command {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    fs::write(&path, input).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_assignment"));
    command.arg(path).env_remove("RUST_LOG");
    command
}

/// Runs the binary on `input`, written to a per-test file named `name`.
fn run_on(name: &str, input: &str) -> Output {
    command_on(name, input).output().unwrap()
}

/// Runs the sample generator with `args`.
fn generate(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_generate"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn reports_accounts_and_keeps_going_past_rejected_rows() {
    let out = run_on(
        "reports.csv",
        r#"type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0
nonsense
"#,
    );
    assert!(out.status.success());
    assert_eq!(
        stdout(&out),
        r#"client,available,held,total,locked
1,1.5000,0.0000,1.5000,false
2,2.0000,0.0000,2.0000,false
"#
    );
    assert!(!stderr(&out).is_empty());
}

#[test]
fn header_only_input_prints_header() {
    let out = run_on("header.csv", "type,client,tx,amount\n");
    assert!(out.status.success());
    assert_eq!(stdout(&out), HEADER);
}

#[test]
fn missing_file_exits_nonzero_with_no_report() {
    let out = Command::new(env!("CARGO_BIN_EXE_assignment"))
        .arg("/no/such/file.csv")
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert_eq!(stdout(&out), "");
}

#[test]
fn directory_input_exits_nonzero_with_no_report() {
    let out = Command::new(env!("CARGO_BIN_EXE_assignment"))
        .arg(env!("CARGO_TARGET_TMPDIR"))
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert_eq!(stdout(&out), "");
}

#[test]
fn generated_file_with_the_most_clients_runs_clean() {
    let input = generate(&[]);
    assert!(input.status.success());
    let out = run_on("generated.csv", &stdout(&input));
    assert!(out.status.success());
    assert_eq!(stderr(&out), "");
    assert_eq!(stdout(&out).lines().count(), usize::from(u16::MAX) + 1);
}

#[test]
fn generated_file_has_the_requested_clients() {
    let out = run_on("few.csv", &stdout(&generate(&["--clients", "4"])));
    assert!(out.status.success());
    assert_eq!(stderr(&out), "");
    assert_eq!(stdout(&out).lines().count(), 5);
}

#[test]
fn generated_file_uses_every_transaction_type() {
    let input = stdout(&generate(&["--clients", "4"]));
    for kind in ["deposit", "withdrawal", "dispute", "resolve", "chargeback"] {
        assert!(input.contains(&format!("\n{kind},")), "{kind}");
    }
}

#[test]
fn generator_refuses_more_clients_than_ids_allow() {
    let out = generate(&["--clients", "65536"]);
    assert!(!out.status.success());
    assert_eq!(stdout(&out), "");
}

#[test]
fn trace_log_records_every_transaction_with_its_outcome() {
    let out = command_on(
        "traced.csv",
        r#"type,client,tx,amount
deposit,1,1,100.0
deposit,1,2,50.0
withdrawal,1,3,20.5
dispute,1,1
resolve,1,1
dispute,1,1
chargeback,1,1
deposit,1,4,1.0
"#,
    )
    .env("RUST_LOG", "assignment=trace")
    .output()
    .unwrap();
    assert!(out.status.success());

    let log = stderr(&out);
    let events: Vec<&str> = log
        .lines()
        .filter_map(|line| line.split_once("assignment::run: transaction processed "))
        .map(|(_, fields)| fields)
        .collect();
    assert_eq!(
        events,
        [
            r#"kind="deposit" client=1 tx=1 amount=100.0000 outcome="applied""#,
            r#"kind="deposit" client=1 tx=2 amount=50.0000 outcome="applied""#,
            r#"kind="withdrawal" client=1 tx=3 amount=20.5000 outcome="applied""#,
            r#"kind="dispute" client=1 tx=1 outcome="applied""#,
            r#"kind="resolve" client=1 tx=1 outcome="applied""#,
            r#"kind="dispute" client=1 tx=1 outcome="applied""#,
            r#"kind="chargeback" client=1 tx=1 outcome="applied""#,
            r#"kind="deposit" client=1 tx=4 amount=1.0000 outcome="rejected" error=ClientFrozen { client: 1, tx: 4 }"#,
        ]
    );
    // Logs must never reach the report on stdout.
    assert_eq!(
        stdout(&out),
        r#"client,available,held,total,locked
1,29.5000,0.0000,29.5000,true
"#
    );
}
