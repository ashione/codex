use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::plan_tool::PlanItemArg;
use codex_protocol::plan_tool::StepStatus;
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
use crate::schema::PlanLifecycleCommandInput;

#[derive(Debug, Clone)]
pub struct PlanLifecycleRequest {
    pub session_id: ThreadId,
    pub turn_id: String,
    pub cwd: AbsolutePathBuf,
    pub transcript_path: Option<PathBuf>,
    pub model: String,
    pub permission_mode: String,
    pub event_name: HookEventName,
    pub plan_source: String,
    pub explanation: Option<String>,
    pub plan: Vec<PlanItemArg>,
    pub previous_plan: Option<Vec<PlanItemArg>>,
    pub plan_text: Option<String>,
}

#[derive(Debug)]
pub struct PlanLifecycleOutcome {
    pub hook_events: Vec<HookCompletedEvent>,
}

pub(crate) fn preview(
    handlers: &[ConfiguredHandler],
    request: &PlanLifecycleRequest,
) -> Vec<HookRunSummary> {
    dispatcher::select_handlers(
        handlers,
        request.event_name,
        Some(request.plan_source.as_str()),
    )
    .into_iter()
    .map(|handler| dispatcher::running_summary(&handler))
    .collect()
}

pub(crate) async fn run(
    handlers: &[ConfiguredHandler],
    shell: &CommandShell,
    request: PlanLifecycleRequest,
) -> PlanLifecycleOutcome {
    let matched = dispatcher::select_handlers(
        handlers,
        request.event_name,
        Some(request.plan_source.as_str()),
    );
    if matched.is_empty() {
        return PlanLifecycleOutcome {
            hook_events: Vec::new(),
        };
    }

    let input_json = match command_input_json(&request) {
        Ok(input_json) => input_json,
        Err(error) => {
            return PlanLifecycleOutcome {
                hook_events: common::serialization_failure_hook_events(
                    matched,
                    Some(request.turn_id),
                    format!("failed to serialize plan lifecycle hook input: {error}"),
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

    PlanLifecycleOutcome {
        hook_events: results.into_iter().map(|result| result.completed).collect(),
    }
}

fn command_input_json(request: &PlanLifecycleRequest) -> Result<String, serde_json::Error> {
    let completed_step_count = request
        .plan
        .iter()
        .filter(|item| matches!(&item.status, StepStatus::Completed))
        .count();
    let in_progress_step_count = request
        .plan
        .iter()
        .filter(|item| matches!(&item.status, StepStatus::InProgress))
        .count();
    let pending_step_count = request
        .plan
        .iter()
        .filter(|item| matches!(&item.status, StepStatus::Pending))
        .count();
    serde_json::to_string(&PlanLifecycleCommandInput {
        session_id: request.session_id.to_string(),
        turn_id: request.turn_id.clone(),
        transcript_path: NullableString::from_path(request.transcript_path.clone()),
        cwd: request.cwd.display().to_string(),
        hook_event_name: event_label(request.event_name).to_string(),
        model: request.model.clone(),
        permission_mode: request.permission_mode.clone(),
        plan_source: request.plan_source.clone(),
        explanation: NullableString::from_string(request.explanation.clone()),
        plan: request.plan.clone(),
        previous_plan: request.previous_plan.clone(),
        completed_step_count,
        in_progress_step_count,
        pending_step_count,
        plan_text: NullableString::from_string(request.plan_text.clone()),
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
        HookEventName::PlanCreated => "PlanCreated",
        HookEventName::PlanUpdated => "PlanUpdated",
        HookEventName::PlanCompleted => "PlanCompleted",
        _ => "PlanLifecycle",
    }
}
