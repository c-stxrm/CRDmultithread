# CRD — Command · Report · Display

A small Rust runtime for hosting independent **jobs** on their own threads.
The owner talks to a job through three channels, and every job speaks the
same protocol:

| Channel | Direction | Meaning |
|---------|-----------|---------|
| **Command** | owner → job | "do this" |
| **Report**  | job → owner | progress or result of one command: `Working`\*, then exactly one `Success` or `Error` |
| **Display** | job → owner | unsolicited news: metrics, state changes, anything the job wants to publish |

The protocol is generic. Each job defines its own command, report and
display types. The owner never needs job-specific terminology to know
whether a command is running, done or failed.

## Layers

```mermaid
flowchart TB
    A["interfaces/crd_job.rs<br/>the contract: CRDJob, CRDJobCommand,<br/>CRDJobReport, CRDJobDisplay, CRDJobOutput"]
    B["templates/crd_job_template.rs<br/>DX layer: CRDJobHandler, CRDJobOutputExt"]
    C["jobs/hello_world.rs<br/>a job: just on_command"]
    D["master/<br/>host: tables, queues, logs, threads"]
    E["main.rs<br/>owner: CLI / UI"]
    A --> B --> C
    A --> D
    C -. "spawned by" .-> D
    D --> E
```

- **Contract** says what a job is. It has no threads and no transport.
- **Template** makes writing a job easy, and a job never writes `run`.
- **Master** gives jobs threads, queues, bookkeeping and logging.
- **main** is the owner. It sends commands and consumes reports and displays.

## Project layout

```text
src/
├── main.rs                      owner: CLI wired to the master
├── interfaces/
│   ├── mod.rs
│   └── crd_job.rs               the shared contract
├── templates/
│   ├── mod.rs
│   └── crd_job_template.rs      CRDJobHandler + output helpers
├── jobs/
│   ├── mod.rs
│   └── hello_world.rs           canonical example job
└── master/
    ├── mod.rs                   module map + re-exports
    ├── ids.rs                   ids, fast integer hasher, thread id
    ├── logs.rs                  CRDMasterLogs + sinks
    ├── command_table.rs         CRDMasterCommandTable
    ├── thread_table.rs          CRDMasterThreadTable
    ├── queues.rs                command/report queue, display queue
    └── runtime.rs               CRDMaster, CRDJobGroup, job threads
```

## Architecture

```mermaid
flowchart LR
    subgraph OWNER["Owner (main.rs / future UI)"]
        CLI["CLI"]
        UI["UI panels"]
    end

    subgraph MASTER["CRDMaster"]
        direction TB
        CQ["CRDMasterCommandReportQueue<br/>round robin dispatch"]
        DQ["CRDMasterDisplayQueue"]
        CT[("CRDMasterCommandTable<br/>in-flight commands")]
        TT[("CRDMasterThreadTable<br/>live threads + jobs")]
        LG["CRDMasterLogs<br/>stdout + JSONL"]
    end

    subgraph GROUP["Job group: hello_world"]
        J0["instance 0<br/>thread hello_world-0"]
        J1["instance 1<br/>thread hello_world-1"]
    end

    CLI -- "send(command)" --> CQ
    CQ -- "register id" --> CT
    CQ -- "1st, 3rd, ..." --> J0
    CQ -. "2nd, 4th, ..." .-> J1
    J0 -- "report" --> CT
    J1 -- "report" --> CT
    J0 -- "report" --> CQ
    J1 -- "report" --> CQ
    J0 -- "display" --> DQ
    J1 -- "display" --> DQ
    CQ -- "reports" --> CLI
    DQ -- "displays" --> UI
    J0 -. "pin / unpin" .-> TT
    J1 -. "pin / unpin" .-> TT
    MASTER -. "every component logs" .-> LG
```

One job group is one job type running on 1..N instances, one OS thread each.
Most of the time N is 1. With N > 1, commands are distributed round robin,
and reports and displays from every instance merge into the group's queues.
Every envelope carries the `instance` it came from.

## Life of a command

```mermaid
sequenceDiagram
    participant O as Owner
    participant Q as CommandReportQueue
    participant T as CommandTable
    participant J as Job (own thread)

    O->>Q: send(GreetMany)
    Q->>T: register(id)
    Q->>J: command (id)
    J->>T: report Working
    J-->>Q: report Working
    Q-->>O: report Working
    J->>T: report Success
    Note over T: terminal report removes the entry
    J-->>Q: report Success
    Q-->>O: report Success
    J-->>O: display GreetingCountChanged (independent)
```

The table is updated on the job's thread, before the owner sees the report.
When the owner reads a terminal report, the command is already gone from the
table.

```mermaid
stateDiagram-v2
    [*] --> Pending: send (registered first)
    Pending --> Pending: Working report
    Pending --> Finished: Success report
    Pending --> Finished: Error report
    Pending --> Abandoned: job ended or panicked
    Finished --> [*]: removed from table
    Abandoned --> [*]: synthetic Error report
```

## Writing a job

Three steps. A job never writes `run`, a thread, or a channel.

```rust
// 1. Command, report and display types.
#[derive(Debug)]
pub enum MyCommand { Ping, Add { a: u64, b: u64 } }

#[derive(Debug)]
pub enum MyReport { Pong, Sum(u64) }

#[derive(Debug)]
pub enum MyDisplay { Started }

// 2. Link the command to its report type.
impl CRDCommand for MyCommand {
    type ReportData = MyReport;
}

// 3. Implement the handler. CRDJob comes for free.
pub struct MyJob;

impl CRDJobHandler for MyJob {
    type Command = MyCommand;
    type CommandId = u64;          // required when hosted by CRDMaster
    type DisplayData = MyDisplay;

    fn on_start(&mut self, output: CRDHandlerOutput<'_, Self>) {
        output.publish_display(MyDisplay::Started);
    }

    fn on_command(&mut self, command: CRDHandlerCommand<Self>, output: CRDHandlerOutput<'_, Self>) {
        let CRDJobCommand { id, data } = command;
        match data {
            MyCommand::Ping => output.report_success_data(id, "pong", MyReport::Pong),
            MyCommand::Add { a, b } => match a.checked_add(b) {
                Some(sum) => output.report_success_data(id, "added", MyReport::Sum(sum)),
                None => output.report_error_message(id, "overflow"),
            },
        }
    }
}
```

`jobs/hello_world.rs` is the full reference. It covers single-step commands,
multi-step commands (`Working` reports, then one terminal report), `Error`
reports carrying data, queries, and every lifecycle hook.

**Rules every job follows**

- A command produces zero or more `Working` reports, then **exactly one**
  `Success` or `Error`. Never report after a terminal report. Never leave a
  command without one.
- Reports answer commands. Displays are for everything else.
- Message is for humans. Data is for programs. Both are optional.

**Output helpers (`CRDJobOutputExt`)**

| Helper | Status | Message | Data |
|--------|--------|---------|------|
| `report_working_message(id, msg)`    | Working | yes  | no   |
| `report_working(id, msg, data)`      | Working | opt. | opt. |
| `report_success_data(id, msg, data)` | Success | yes  | yes  |
| `report_success(id, msg, data)`      | Success | opt. | opt. |
| `report_error_message(id, msg)`      | Error   | yes  | no   |
| `report_error(id, msg, data)`        | Error   | opt. | opt. |
| `publish_display(data)`              | (display) | -  | yes  |

A job that needs full control can implement `CRDJob::run` by hand instead.
A type implements either `CRDJobHandler` or `CRDJob`, never both.

## Hosting a job

```rust
let logs = CRDMasterLogs::new();
logs.add_sink(CRDStdoutSink::new(CRDLogLevel::Info));

let master = CRDMaster::new(logs);

// one instance on one thread
let mut group = master.spawn("hello_world", vec![CRDHelloWorld::new()])?;

// or three instances, round robin
// let mut group = master.spawn("hello_world", vec![a, b, c])?;

let id = group.queue().send(CRDHelloWorldCommand::GetGreetingCount)?;

while let Ok(envelope) = group.queue().recv_report_timeout(Duration::from_secs(1)) {
    println!("{:?}", envelope.report);
    if envelope.report.command_id == id && crd_is_terminal(envelope.report.status) { break; }
}

// hand displays to a UI thread
let displays = group.take_display_queue();

group.shutdown(); // also runs on drop
```

Requirements on a hosted job: `CommandId = u64`, and `Command`, `ReportData`
and `DisplayData` implement `Debug`.

## The master mechanisms

| Mechanism | Type | Purpose |
|-----------|------|---------|
| Command table | `CRDMasterCommandTable` | commands sent and not yet finished, with their report history |
| Thread table  | `CRDMasterThreadTable`  | live threads and the jobs pinned to them |
| Command/report queue | `CRDMasterCommandReportQueue<C, R>` | per job group: round-robin dispatch out, merged reports in |
| Display queue | `CRDMasterDisplayQueue<D>` | per job group: merged unsolicited displays |
| Logs | `CRDMasterLogs` | colored multi-sink logging |

### Command table: built for speed

```text
 command id ──► id & 15 ──► shard 0 .. shard 15
                             each: Mutex<HashMap<u64, Record, CRDIdBuildHasher>>
                             each: 64-byte aligned (no false sharing)
```

- **Sharded:** 16 independent locks. Ids are sequential, so load spreads
  evenly and different commands almost never contend.
- **Integer hasher:** one multiply instead of SipHash. The keys are ids we
  generated, so hash-flooding resistance buys nothing.
- **Lock-free counters:** id generation and the pending count are atomics.
- **Short critical sections:** a lock covers one map operation.
- **Type-erased:** it stores ids, status, timing and messages, never job
  payloads, so one table serves every job type.
- **Bounded:** each record keeps its newest 32 report summaries.
  `report_count` still counts all of them.

### Thread table

A thread registers itself on start and unregisters on exit, even on panic.
It only ever holds threads that are still working. A thread can host
several jobs; the master currently starts one thread per job instance.

### Queues

- `send` assigns an id, **registers the command first**, then dispatches. A
  fast job can never report on an id the table has not seen.
- An instance that ended is skipped and the command is retried on the next
  one. If none accepts it, the command comes back in `CRDSendError`.
- Each queue has one consumer. Queues of different jobs are independent, so
  several consumers (UI panels) can read different jobs in parallel.

### Failure handling

If a job ends or panics with commands still pending, each one is removed
from the table, logged at `ERROR`, and answered with a synthetic `Error`
report. The owner is never left waiting. A report for an id that is not
pending (unknown, or sent after the terminal one) is logged as a protocol
violation.

## Logging

```mermaid
flowchart LR
    C["CRDMasterLogs<br/>(clone anywhere)"] --> F{"enabled?<br/>lock-free level check"}
    F -- no --> X["dropped, nothing built"]
    F -- yes --> S["sinks, serialised"]
    S --> O["CRDStdoutSink<br/>colored, own min level"]
    S --> J["CRDJsonlSink<br/>logs/crd_master.jsonl"]
    S --> N["your CRDLogSink<br/>add_sink(...)"]
```

A new destination is a type that implements `CRDLogSink` (`min_level`,
`write`, optional `flush`). The logger never changes.

Stdout line:

```text
12:00:00.123 INFO  tid=4242   crd.report command finished command=1 status=success job=hello_world instance=0 reports=2 elapsed_us=412
```

JSONL line (one object per line, flushed per record):

```json
{"ts":"2026-11-09T12:00:00.123Z","ts_ms":1794225600123,"level":"INFO","source":"crd.report","tid":4242,"msg":"command finished","fields":{"command":1,"status":"success","job":"hello_world","instance":0,"reports":2,"elapsed_us":412}}
```

| Level | Used for |
|-------|----------|
| `TRACE` / `DEBUG` | command dispatch, `Working` reports, displays, payload dumps |
| `INFO`  | job group and thread lifecycle, terminal `Success` reports |
| `WARN`  | `Error` reports, unreachable instances, protocol violations |
| `ERROR` | thread creation failure, panics, abandoned commands |

Guard expensive messages with `logs.enabled(level)`. Color turns on only
when stdout is a terminal and `NO_COLOR` is unset.

## Running

```powershell
cargo run

# live detail on stdout (the file always records everything)
$env:CRD_LOG = "debug"; cargo run

# read the JSONL log
Get-Content logs\crd_master.jsonl -Tail 20
```

| CLI command | Effect |
|-------------|--------|
| `greet <name>` | single-step command |
| `greetmany a,b,c` | multi-step command: `Working` reports, then one terminal report |
| `count` | query |
| `cmds` | commands still pending (command table) |
| `threads` | live job threads and pinned jobs (thread table) |
| `help`, `exit` | |

| Setting | Default | Meaning |
|---------|---------|---------|
| `CRD_LOG` | `warn` | stdout level: trace, debug, info, warn, error |
| `NO_COLOR` | unset | any value disables colors |
| `logs/crd_master.jsonl` | | JSONL file, appended to, always `trace` |

Windows colors need the `Win32_System_Console` feature of `windows-sys`:

```powershell
cargo add windows-sys --features Win32_System_Console
```

## Design principles

| Principle | Where |
|-----------|-------|
| **Single responsibility** | one file per mechanism: ids, logs, command table, thread table, queues, runtime |
| **Open / closed** | new log destination = new `CRDLogSink`. New job = new `CRDJobHandler`. Neither touches existing code |
| **Liskov** | every `CRDJobHandler` is a `CRDJob` through one blanket impl, and the master hosts any `CRDJob` |
| **Interface segregation** | jobs see only `CRDJobOutput`. Tables, queues and logs are invisible to them |
| **Dependency inversion** | jobs depend on the `CRDJobOutput` abstraction. The master supplies the implementation. The logger depends on the `CRDLogSink` trait |

## Limits and roadmap

- A group's instance count is fixed at spawn (`add_instance` exists
  internally, with no public scale-up yet).
- Each report and display queue has one consumer (`mpsc`).
- `Debug` is required on job payloads for logging.
- Command ids are master-assigned `u64` counters.
- Not yet built: JSON configuration, log rotation, graceful per-command
  cancellation.