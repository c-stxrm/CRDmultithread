mod interfaces;
mod jobs;
mod master;
mod templates;

use std::fmt::Debug;
use std::io::{self, Write};
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use crate::jobs::hello_world::{CRDHelloWorld, CRDHelloWorldCommand};
use crate::master::{
    crd_field, crd_is_terminal, CRDJobGroup, CRDJsonlSink, CRDLogLevel, CRDMaster,
    CRDMasterLogs, CRDMasterReport, CRDStdoutSink,
};

/// JSONL log file, relative to the working directory.
const CRD_LOG_FILE: &str = "logs/crd_master.jsonl";

/// How long the CLI waits for a command to finish before giving up.
const CRD_REPORT_WAIT: Duration = Duration::from_secs(2);

// ============================================================================
// LOGGING SETUP
// ============================================================================

/// Builds the logger: colored stdout plus a JSONL file.
///
/// Stdout level comes from the `CRD_LOG` environment variable
/// (trace|debug|info|warn|error), default `warn`. The file always gets
/// everything (`trace`).
fn crd_build_logs() -> CRDMasterLogs {
    let logs = CRDMasterLogs::new();

    let stdout_level = std::env::var("CRD_LOG")
        .ok()
        .and_then(|value| CRDLogLevel::parse(&value))
        .unwrap_or(CRDLogLevel::Warn);

    logs.add_sink(CRDStdoutSink::new(stdout_level));

    match CRDJsonlSink::open(CRD_LOG_FILE, CRDLogLevel::Trace) {
        Ok(sink) => logs.add_sink(sink),
        Err(error) => logs.warn(
            "crd.main",
            &format!("cannot open log file {}: {}", CRD_LOG_FILE, error),
        ),
    }

    logs
}

// ============================================================================
// OWNER VIEW (what the CLI shows)
// ============================================================================

fn crd_print_report<R: Debug>(envelope: &CRDMasterReport<R>) {
    let report = &envelope.report;

    println!(
        "  <- #{} {:?} (instance {}) message={:?} data={:?}",
        report.command_id, report.status, envelope.instance, report.message, report.data
    );
}

fn crd_print_displays(group: &CRDJobGroup<CRDHelloWorld>) {
    if let Some(display) = group.display() {
        for item in display.drain() {
            println!("  ~ display (instance {}) {:?}", item.instance, item.data);
        }
    }
}

/// Prints reports until command `id` reaches a terminal report.
fn crd_wait_for_command(group: &CRDJobGroup<CRDHelloWorld>, id: u64) {
    loop {
        match group.queue().recv_report_timeout(CRD_REPORT_WAIT) {
            Ok(envelope) => {
                crd_print_report(&envelope);

                if envelope.report.command_id == id && crd_is_terminal(envelope.report.status) {
                    return;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                println!("  (command #{} has not finished after {:?})", id, CRD_REPORT_WAIT);
                return;
            }
            Err(RecvTimeoutError::Disconnected) => {
                println!("  (the job ended)");
                return;
            }
        }
    }
}

fn crd_print_help() {
    println!("  greet <name>      greet one person");
    println!("  greetmany a,b,c   greet several names (comma separated)");
    println!("  count             ask for the greeting count");
    println!("  cmds              commands still pending (command table)");
    println!("  threads           live job threads (thread table)");
    println!("  help              show this help");
    println!("  exit              stop the job");
}

// ============================================================================
// MAIN
// ============================================================================

fn main() {
    let logs = crd_build_logs();
    logs.info("crd.main", "application started");

    let master = CRDMaster::new(logs.clone());

    // One HelloWorld instance on its own thread. For round robin across
    // several threads, pass more instances:
    //     vec![CRDHelloWorld::new(), CRDHelloWorld::new()]
    let mut hello = match master.spawn("hello_world", vec![CRDHelloWorld::new()]) {
        Ok(group) => group,
        Err(error) => {
            println!("Cannot start the job: {}", error);
            logs.error("crd.main", &format!("cannot start the job: {}", error));
            logs.flush();
            return;
        }
    };

    println!("==================================");
    println!("       CSTXRM CRD INTERFACE");
    println!("==================================");
    crd_print_help();
    println!("==================================");
    println!("log file: {}  (set CRD_LOG=debug for live detail)", CRD_LOG_FILE);

    loop {
        crd_print_displays(&hello);

        print!("CRD> ");
        let _ = io::stdout().flush();

        let mut input = String::new();

        match io::stdin().read_line(&mut input) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        // Split "<verb> <rest>". The input is already trimmed, so a bare
        // "greet" yields an empty `rest` and reaches the job, which rejects it.
        let (verb, rest) = match input.split_once(char::is_whitespace) {
            Some((verb, rest)) => (verb, rest.trim()),
            None => (input, ""),
        };

        let command = match verb {
            "help" => {
                crd_print_help();
                continue;
            }
            "exit" => {
                logs.info("crd.main", "shutdown requested");
                break;
            }
            "cmds" => {
                let pending = master.commands().snapshot();
                println!("  {} pending command(s)", pending.len());
                for record in pending {
                    println!(
                        "  #{} {} on {} | reports={} | age={:.1?}",
                        record.id,
                        record.job_name,
                        record.job,
                        record.report_count,
                        record.sent_at.elapsed()
                    );
                }
                continue;
            }
            "threads" => {
                let live = master.threads().snapshot();
                println!("  {} live thread(s)", live.len());
                for thread in live {
                    let jobs: Vec<String> = thread.jobs.iter().map(|job| job.to_string()).collect();
                    println!(
                        "  tid={} name={} jobs={:?} up={:.1?}",
                        thread.thread_id,
                        thread.name,
                        jobs,
                        thread.started_at.elapsed()
                    );
                }
                continue;
            }
            "greet" => CRDHelloWorldCommand::Greet {
                name: rest.to_string(),
            },
            "greetmany" => CRDHelloWorldCommand::GreetMany {
                names: if rest.is_empty() {
                    Vec::new()
                } else {
                    rest.split(',').map(|name| name.trim().to_string()).collect()
                },
            },
            "count" => CRDHelloWorldCommand::GetGreetingCount,
            _ => {
                println!("Unknown command. Type 'help'.");
                logs.warn_with(
                    "crd.main",
                    "unknown cli input",
                    &[crd_field("input", input)],
                );
                continue;
            }
        };

        match hello.queue().send(command) {
            Ok(id) => crd_wait_for_command(&hello, id),
            Err(error) => {
                println!("Job is no longer accepting commands: {}", error);
                logs.error("crd.main", "command delivery failed: job disconnected");
                break;
            }
        }
    }

    // Closes the command input, lets the job finish, and joins its thread.
    hello.shutdown();
    crd_print_displays(&hello);

    logs.info_with(
        "crd.main",
        "application terminated",
        &[
            crd_field("pending_commands", master.commands().len()),
            crd_field("live_threads", master.threads().len()),
        ],
    );
    logs.flush();
}