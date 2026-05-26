use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HookCompletedEvent;
use codex_protocol::protocol::HookEventName;
use codex_protocol::protocol::HookOutputEntry;
use codex_protocol::protocol::HookOutputEntryKind;
use codex_protocol::protocol::HookRunStatus;
use codex_protocol::protocol::HookRunSummary;
use codex_utils_absolute_path::AbsolutePathBuf;

use super::common;
use crate::engine::CommandShell;
use crate::engine::ConfiguredHandler;
use crate::engine::command_runner::CommandRunResult;
use crate::engine::dispatcher;
use crate::schema::NullableString;
use crate::schema::TaskLifecycleCommandInput;

#[derive(Debug, Clone)]
pub struct TaskLifecycleRequest {
    pub session_id: ThreadId,
    pub turn_id: String,
    pub cwd: AbsolutePathBuf,
    pub transcript_path: Option<PathBuf>,
    pub model: String,
    pub permission_mode: String,
    pub event_name: HookEventName,
    pub task_kind: String,
    pub last_agent_message: Option<String>,
    pub completed_at: Option<i64>,
    pub duration_ms: Option<i64>,
    pub time_to_first_token_ms: Option<i64>,
}

#[derive(Debug)]
pub struct TaskLifecycleOutcome {
    pub hook_events: Vec<HookCompletedEvent>,
}

pub(crate) fn preview(
    handlers: &[ConfiguredHandler],
    request: &TaskLifecycleRequest,
) -> Vec<HookRunSummary> {
    dispatcher::select_handlers(
        handlers,
        request.event_name,
        Some(request.task_kind.as_str()),
    )
    .into_iter()
    .map(|handler| dispatcher::running_summary(&handler))
    .collect()
}

pub(crate) async fn run(
    handlers: &[ConfiguredHandler],
    shell: &CommandShell,
    request: TaskLifecycleRequest,
) -> TaskLifecycleOutcome {
    let matched = dispatcher::select_handlers(
        handlers,
        request.event_name,
        Some(request.task_kind.as_str()),
    );
    if matched.is_empty() {
        return TaskLifecycleOutcome {
            hook_events: Vec::new(),
        };
    }

    let input_json = match command_input_json(&request) {
        Ok(input_json) => input_json,
        Err(error) => {
            return TaskLifecycleOutcome {
                hook_events: common::serialization_failure_hook_events(
                    matched,
                    Some(request.turn_id),
                    format!("failed to serialize task lifecycle hook input: {error}"),
                ),
            };
        }
    };
    let results = dispatcher::execute_handlers(
        shell,
        matched,
        input_json,
        request.cwd.as_path(),
        Some(request.turn_id),
        parse_completed,
    )
    .await;

    TaskLifecycleOutcome {
        hook_events: results.into_iter().map(|result| result.completed).collect(),
    }
}

fn command_input_json(request: &TaskLifecycleRequest) -> Result<String, serde_json::Error> {
    serde_json::to_string(&TaskLifecycleCommandInput {
        session_id: request.session_id.to_string(),
        turn_id: request.turn_id.clone(),
        transcript_path: NullableString::from_path(request.transcript_path.clone()),
        cwd: request.cwd.display().to_string(),
        hook_event_name: event_label(request.event_name).to_string(),
        model: request.model.clone(),
        permission_mode: request.permission_mode.clone(),
        task_kind: request.task_kind.clone(),
        last_agent_message: NullableString::from_string(request.last_agent_message.clone()),
        completed_at: request.completed_at,
        duration_ms: request.duration_ms,
        time_to_first_token_ms: request.time_to_first_token_ms,
    })
}

fn parse_completed(
    handler: &ConfiguredHandler,
    run_result: CommandRunResult,
    turn_id: Option<String>,
) -> dispatcher::ParsedHandler<()> {
    let mut entries = Vec::new();
    let mut status = HookRunStatus::Completed;

    match run_result.error.as_deref() {
        Some(error) => {
            status = HookRunStatus::Failed;
            entries.push(HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text: error.to_string(),
            });
        }
        None => match run_result.exit_code {
            Some(0) => {}
            Some(code) => {
                status = HookRunStatus::Failed;
                entries.push(HookOutputEntry {
                    kind: HookOutputEntryKind::Error,
                    text: common::trimmed_non_empty(&run_result.stderr)
                        .unwrap_or_else(|| format!("hook exited with code {code}")),
                });
            }
            None => {
                status = HookRunStatus::Failed;
                entries.push(HookOutputEntry {
                    kind: HookOutputEntryKind::Error,
                    text: "hook process terminated without an exit code".to_string(),
                });
            }
        },
    }

    dispatcher::ParsedHandler {
        completed: HookCompletedEvent {
            turn_id,
            run: dispatcher::completed_summary(handler, &run_result, status, entries),
        },
        data: (),
        completion_order: 0,
    }
}

fn event_label(event_name: HookEventName) -> &'static str {
    match event_name {
        HookEventName::TaskCreated => "TaskCreated",
        HookEventName::TaskCompleted => "TaskCompleted",
        _ => "TaskLifecycle",
    }
}
