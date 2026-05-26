use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use app_test_support::McpProcess;
use app_test_support::to_response;
use codex_app_server_protocol::ConfigBatchWriteParams;
use codex_app_server_protocol::ConfigEdit;
use codex_app_server_protocol::HookCompletedNotification;
use codex_app_server_protocol::HookEventName;
use codex_app_server_protocol::HookRunStatus;
use codex_app_server_protocol::HooksListParams;
use codex_app_server_protocol::HooksListResponse;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::MergeStrategy;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnPlanStep;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_app_server_protocol::TurnPlanUpdateOperation;
use codex_app_server_protocol::TurnPlanUpdateParams;
use codex_app_server_protocol::TurnPlanUpdateResponse;
use codex_app_server_protocol::TurnPlanUpdatedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::UserInput as V2UserInput;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::skip_if_windows;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_plan_update_emits_notification_and_plan_hook() -> Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_windows!(Ok(()));

    let server = responses::start_mock_server().await;
    responses::mount_response_once(
        &server,
        responses::sse_response(responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_assistant_message("msg-1", "done"),
            responses::ev_completed("resp-1"),
        ]))
        .set_delay(Duration::from_secs(5)),
    )
    .await;

    let codex_home = TempDir::new()?;
    let hook_log_path = codex_home.path().join("plan_hook_log.jsonl");
    let hook_script_path = codex_home.path().join("plan_hook.py");
    write_plan_hook_script(&hook_script_path, &hook_log_path)?;
    create_config_toml(codex_home.path(), &server.uri(), &hook_script_path)?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_TIMEOUT, mcp.initialize()).await??;
    trust_plan_updated_hook(&mut mcp, codex_home.path()).await?;

    let thread = start_thread(&mut mcp).await?;
    let turn = start_turn(&mut mcp, &thread.id).await?;
    let started = read_turn_started_notification(&mut mcp).await?;
    assert_eq!(started.thread_id, thread.id);
    assert_eq!(started.turn.id, turn.id);

    let append_id = mcp
        .send_turn_plan_update_request(TurnPlanUpdateParams {
            thread_id: thread.id.clone(),
            expected_turn_id: turn.id.clone(),
            explanation: Some("external harness update".to_string()),
            operations: vec![TurnPlanUpdateOperation::Append {
                step: "Run external validation".to_string(),
                status: None,
            }],
        })
        .await?;
    let append_response: JSONRPCResponse = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(append_id)),
    )
    .await??;
    let append_response: TurnPlanUpdateResponse = to_response(append_response)?;
    assert_eq!(
        append_response.plan,
        vec![TurnPlanStep {
            step: "Run external validation".to_string(),
            status: TurnPlanStepStatus::Pending,
        }]
    );

    let hook_completed = read_plan_updated_hook_completed(&mut mcp).await?;
    assert_eq!(hook_completed.turn_id, Some(turn.id.clone()));
    assert_eq!(hook_completed.run.status, HookRunStatus::Completed);

    let plan_updated = read_plan_updated_notification(&mut mcp).await?;
    assert_eq!(plan_updated.thread_id, thread.id);
    assert_eq!(plan_updated.turn_id, turn.id);
    assert_eq!(
        plan_updated.plan,
        vec![TurnPlanStep {
            step: "Run external validation".to_string(),
            status: TurnPlanStepStatus::Pending,
        }]
    );

    let payloads = hook_payloads(&hook_log_path)?;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["hook_event_name"], "PlanUpdated");
    assert_eq!(payloads[0]["plan_source"], "external");
    assert_eq!(payloads[0]["pending_step_count"], 1);

    let complete_id = mcp
        .send_turn_plan_update_request(TurnPlanUpdateParams {
            thread_id: plan_updated.thread_id,
            expected_turn_id: plan_updated.turn_id,
            explanation: None,
            operations: vec![TurnPlanUpdateOperation::Update {
                index: 0,
                step: None,
                status: Some(TurnPlanStepStatus::Completed),
            }],
        })
        .await?;
    let complete_response: JSONRPCResponse = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(complete_id)),
    )
    .await??;
    let complete_response: TurnPlanUpdateResponse = to_response(complete_response)?;
    assert_eq!(
        complete_response.plan[0].status,
        TurnPlanStepStatus::Completed
    );

    let rejected_id = mcp
        .send_turn_plan_update_request(TurnPlanUpdateParams {
            thread_id: thread.id.clone(),
            expected_turn_id: turn.id.clone(),
            explanation: None,
            operations: vec![TurnPlanUpdateOperation::Update {
                index: 0,
                step: Some("Rewrite completed history".to_string()),
                status: Some(TurnPlanStepStatus::Pending),
            }],
        })
        .await?;
    let rejected: JSONRPCError = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(rejected_id)),
    )
    .await??;
    assert_eq!(rejected.error.code, -32600);
    assert!(
        rejected.error.message.contains("already completed"),
        "unexpected error: {}",
        rejected.error.message
    );

    Ok(())
}

async fn trust_plan_updated_hook(mcp: &mut McpProcess, codex_home: &Path) -> Result<()> {
    let hook_list_id = mcp
        .send_hooks_list_request(HooksListParams {
            cwds: vec![codex_home.to_path_buf()],
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(hook_list_id)),
    )
    .await??;
    let HooksListResponse { data } = to_response(response)?;
    let hook = data
        .first()
        .and_then(|entry| {
            entry
                .hooks
                .iter()
                .find(|hook| hook.event_name == HookEventName::PlanUpdated)
        })
        .ok_or_else(|| anyhow!("expected PlanUpdated hook to be listed"))?
        .clone();

    let write_id = mcp
        .send_config_batch_write_request(ConfigBatchWriteParams {
            edits: vec![ConfigEdit {
                key_path: "hooks.state".to_string(),
                value: serde_json::json!({
                    hook.key: {
                        "trusted_hash": hook.current_hash
                    }
                }),
                merge_strategy: MergeStrategy::Upsert,
            }],
            file_path: None,
            expected_version: None,
            reload_user_config: true,
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(write_id)),
    )
    .await??;
    let _: codex_app_server_protocol::ConfigWriteResponse = to_response(response)?;
    Ok(())
}

async fn start_thread(mcp: &mut McpProcess) -> Result<codex_app_server_protocol::Thread> {
    let request_id = mcp
        .send_thread_start_request(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    Ok(to_response::<ThreadStartResponse>(response)?.thread)
}

async fn start_turn(
    mcp: &mut McpProcess,
    thread_id: &str,
) -> Result<codex_app_server_protocol::Turn> {
    let request_id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![V2UserInput::Text {
                text: "keep this turn active briefly".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    Ok(to_response::<TurnStartResponse>(response)?.turn)
}

async fn read_plan_updated_hook_completed(
    mcp: &mut McpProcess,
) -> Result<HookCompletedNotification> {
    let notification = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_matching_notification("PlanUpdated hook/completed", |notification| {
            if notification.method != "hook/completed" {
                return false;
            }
            notification
                .params
                .clone()
                .and_then(|params| serde_json::from_value::<HookCompletedNotification>(params).ok())
                .is_some_and(|payload| payload.run.event_name == HookEventName::PlanUpdated)
        }),
    )
    .await??;
    notification_params(notification)
}

async fn read_plan_updated_notification(
    mcp: &mut McpProcess,
) -> Result<TurnPlanUpdatedNotification> {
    let notification = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/plan/updated"),
    )
    .await??;
    notification_params(notification)
}

async fn read_turn_started_notification(mcp: &mut McpProcess) -> Result<TurnStartedNotification> {
    let notification = timeout(
        DEFAULT_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;
    notification_params(notification)
}

fn notification_params<T>(notification: JSONRPCNotification) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let params = notification
        .params
        .ok_or_else(|| anyhow!("notification must include params"))?;
    Ok(serde_json::from_value(params)?)
}

fn hook_payloads(log_path: &Path) -> Result<Vec<serde_json::Value>> {
    let log = std::fs::read_to_string(log_path)
        .with_context(|| format!("read hook log {}", log_path.display()))?;
    log.lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

fn write_plan_hook_script(script_path: &Path, log_path: &Path) -> Result<()> {
    std::fs::write(
        script_path,
        format!(
            r#"import json
from pathlib import Path
import sys

payload = json.load(sys.stdin)
with Path(r"{log_path}").open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload, sort_keys=True) + "\n")
"#,
            log_path = log_path.display(),
        ),
    )?;
    Ok(())
}

fn create_config_toml(codex_home: &Path, server_uri: &str, hook_script_path: &Path) -> Result<()> {
    std::fs::write(
        codex_home.join("config.toml"),
        format!(
            r#"
model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[features]
hooks = true

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0

[hooks]

[[hooks.PlanUpdated]]
matcher = "external"

[[hooks.PlanUpdated.hooks]]
type = "command"
command = "python3 {hook_script_path}"
timeout = 5
"#,
            hook_script_path = hook_script_path.display(),
        ),
    )?;
    Ok(())
}
