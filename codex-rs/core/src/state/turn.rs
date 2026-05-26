//! Turn-scoped state and active turn metadata scaffolding.

use codex_sandboxing::policy_transforms::merge_permission_profiles;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use codex_extension_api::ExtensionData;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::request_permissions::RequestPermissionProfile;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use codex_protocol::request_user_input::RequestUserInputResponse;
use codex_rmcp_client::ElicitationResponse;
use codex_utils_absolute_path::AbsolutePathBuf;
use rmcp::model::RequestId;
use tokio::sync::oneshot;

use crate::session::TurnInputQueue;
use crate::session::turn_context::TurnContext;
use crate::tasks::AnySessionTask;
use codex_protocol::models::AdditionalPermissionProfile;
use codex_protocol::plan_tool::ExternalPlanUpdateOperation;
use codex_protocol::plan_tool::PlanItemArg;
use codex_protocol::plan_tool::StepStatus;
use codex_protocol::plan_tool::UpdatePlanArgs;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::protocol::TokenUsage;

/// Metadata about the currently running turn.
pub(crate) struct ActiveTurn {
    pub(crate) tasks: IndexMap<String, RunningTask>,
    pub(crate) turn_state: Arc<Mutex<TurnState>>,
    pub(crate) pending_turn_context: Option<Arc<TurnContext>>,
}

/// Whether mailbox deliveries should still be folded into the current turn.
///
/// State machine:
/// - A turn starts in `CurrentTurn`, so queued child mail can join the next
///   model request for that turn.
/// - After user-visible terminal output is recorded, we switch to `NextTurn`
///   to leave late child mail queued instead of extending an already shown
///   answer.
/// - If the same task later gets explicit same-turn work again (a steered user
///   prompt or a tool call after an untagged preamble), we reopen `CurrentTurn`
///   so that pending child mail is drained into that follow-up request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum MailboxDeliveryPhase {
    /// Incoming mailbox messages can still be consumed by the current turn.
    #[default]
    CurrentTurn,
    /// The current turn already emitted visible final answer text; mailbox
    /// messages should remain queued for a later turn.
    NextTurn,
}

impl Default for ActiveTurn {
    fn default() -> Self {
        Self {
            tasks: IndexMap::new(),
            turn_state: Arc::new(Mutex::new(TurnState::default())),
            pending_turn_context: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskKind {
    Regular,
    Review,
    Compact,
}

pub(crate) struct RunningTask {
    pub(crate) done: Arc<Notify>,
    pub(crate) kind: TaskKind,
    pub(crate) task: Arc<dyn AnySessionTask>,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) handle: AbortOnDropHandle<()>,
    pub(crate) turn_context: Arc<TurnContext>,
    pub(crate) turn_extension_data: Arc<ExtensionData>,
    // Timer recorded when the task drops to capture the full turn duration.
    pub(crate) _timer: Option<codex_otel::Timer>,
}

pub(crate) struct RemovedTask {
    pub(crate) kind: TaskKind,
    pub(crate) records_turn_token_usage_on_span: bool,
    pub(crate) active_turn_is_empty: bool,
}

impl ActiveTurn {
    pub(crate) fn add_task(&mut self, task: RunningTask) {
        let sub_id = task.turn_context.sub_id.clone();
        if self
            .pending_turn_context
            .as_ref()
            .is_some_and(|turn_context| turn_context.sub_id == sub_id)
        {
            self.pending_turn_context = None;
        }
        self.tasks.insert(sub_id, task);
    }

    pub(crate) fn remove_task(&mut self, sub_id: &str) -> Option<RemovedTask> {
        let task = self.tasks.swap_remove(sub_id)?;
        let records_turn_token_usage_on_span = task.task.records_turn_token_usage_on_span();
        task.handle.detach();
        Some(RemovedTask {
            kind: task.kind,
            records_turn_token_usage_on_span,
            active_turn_is_empty: self.tasks.is_empty(),
        })
    }

    pub(crate) fn drain_tasks(&mut self) -> Vec<RunningTask> {
        self.tasks.drain(..).map(|(_, task)| task).collect()
    }
}

/// Mutable state for a single turn.
#[derive(Default)]
pub(crate) struct TurnState {
    pending_approvals: HashMap<String, oneshot::Sender<ReviewDecision>>,
    pending_request_permissions: HashMap<String, PendingRequestPermissions>,
    pending_user_input: HashMap<String, oneshot::Sender<RequestUserInputResponse>>,
    pending_elicitations: HashMap<(String, RequestId), oneshot::Sender<ElicitationResponse>>,
    pending_dynamic_tools: HashMap<String, oneshot::Sender<DynamicToolResponse>>,
    pub(crate) pending_input: TurnInputQueue,
    current_plan: Option<UpdatePlanArgs>,
    plan_completed_hook_emitted: bool,
    mailbox_delivery_phase: MailboxDeliveryPhase,
    granted_permissions: Option<AdditionalPermissionProfile>,
    strict_auto_review_enabled: bool,
    pub(crate) tool_calls: u64,
    pub(crate) has_memory_citation: bool,
    pub(crate) token_usage_at_turn_start: TokenUsage,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanLifecycleTransition {
    pub(crate) created: bool,
    pub(crate) updated: bool,
    pub(crate) completed: bool,
    pub(crate) previous_plan: Option<Vec<PlanItemArg>>,
    pub(crate) current_plan: UpdatePlanArgs,
}

pub(crate) struct PendingRequestPermissions {
    pub(crate) tx_response: oneshot::Sender<RequestPermissionsResponse>,
    pub(crate) requested_permissions: RequestPermissionProfile,
    pub(crate) cwd: AbsolutePathBuf,
}

impl TurnState {
    pub(crate) fn insert_pending_approval(
        &mut self,
        key: String,
        tx: oneshot::Sender<ReviewDecision>,
    ) -> Option<oneshot::Sender<ReviewDecision>> {
        self.pending_approvals.insert(key, tx)
    }

    pub(crate) fn remove_pending_approval(
        &mut self,
        key: &str,
    ) -> Option<oneshot::Sender<ReviewDecision>> {
        self.pending_approvals.remove(key)
    }

    pub(crate) fn clear_pending_waiters(&mut self) {
        self.pending_approvals.clear();
        self.pending_request_permissions.clear();
        self.pending_user_input.clear();
        self.pending_elicitations.clear();
        self.pending_dynamic_tools.clear();
    }

    pub(crate) fn apply_plan_snapshot(
        &mut self,
        update: UpdatePlanArgs,
    ) -> PlanLifecycleTransition {
        let created = self.current_plan.is_none();
        let previous_plan = self.current_plan.as_ref().map(|plan| plan.plan.clone());
        let completed = plan_is_completed(&update.plan) && !self.plan_completed_hook_emitted;
        if completed {
            self.plan_completed_hook_emitted = true;
        }
        self.current_plan = Some(update.clone());
        PlanLifecycleTransition {
            created,
            updated: true,
            completed,
            previous_plan,
            current_plan: update,
        }
    }

    pub(crate) fn apply_external_plan_update(
        &mut self,
        explanation: Option<String>,
        operations: Vec<ExternalPlanUpdateOperation>,
    ) -> Result<PlanLifecycleTransition, String> {
        if operations.is_empty() {
            return Err("operations must not be empty".to_string());
        }
        let mut next = self.current_plan.clone().unwrap_or(UpdatePlanArgs {
            explanation: None,
            plan: Vec::new(),
        });
        if explanation.is_some() {
            next.explanation = explanation;
        }
        for operation in operations {
            match operation {
                ExternalPlanUpdateOperation::Append { step, status } => {
                    next.plan.push(PlanItemArg {
                        step,
                        status: status.unwrap_or(StepStatus::Pending),
                    });
                }
                ExternalPlanUpdateOperation::Update {
                    index,
                    step,
                    status,
                } => {
                    let Some(item) = next.plan.get_mut(index) else {
                        return Err(format!("plan item index {index} is out of range"));
                    };
                    if item.status == StepStatus::Completed {
                        return Err(format!("plan item index {index} is already completed"));
                    }
                    if let Some(step) = step {
                        item.step = step;
                    }
                    if let Some(status) = status {
                        item.status = status;
                    }
                }
            }
        }
        Ok(self.apply_plan_snapshot(next))
    }

    pub(crate) fn insert_pending_request_permissions(
        &mut self,
        key: String,
        pending_request_permissions: PendingRequestPermissions,
    ) -> Option<PendingRequestPermissions> {
        self.pending_request_permissions
            .insert(key, pending_request_permissions)
    }

    pub(crate) fn remove_pending_request_permissions(
        &mut self,
        key: &str,
    ) -> Option<PendingRequestPermissions> {
        self.pending_request_permissions.remove(key)
    }

    pub(crate) fn insert_pending_user_input(
        &mut self,
        key: String,
        tx: oneshot::Sender<RequestUserInputResponse>,
    ) -> Option<oneshot::Sender<RequestUserInputResponse>> {
        self.pending_user_input.insert(key, tx)
    }

    pub(crate) fn remove_pending_user_input(
        &mut self,
        key: &str,
    ) -> Option<oneshot::Sender<RequestUserInputResponse>> {
        self.pending_user_input.remove(key)
    }

    pub(crate) fn insert_pending_elicitation(
        &mut self,
        server_name: String,
        request_id: RequestId,
        tx: oneshot::Sender<ElicitationResponse>,
    ) -> Option<oneshot::Sender<ElicitationResponse>> {
        self.pending_elicitations
            .insert((server_name, request_id), tx)
    }

    pub(crate) fn remove_pending_elicitation(
        &mut self,
        server_name: &str,
        request_id: &RequestId,
    ) -> Option<oneshot::Sender<ElicitationResponse>> {
        self.pending_elicitations
            .remove(&(server_name.to_string(), request_id.clone()))
    }

    pub(crate) fn insert_pending_dynamic_tool(
        &mut self,
        key: String,
        tx: oneshot::Sender<DynamicToolResponse>,
    ) -> Option<oneshot::Sender<DynamicToolResponse>> {
        self.pending_dynamic_tools.insert(key, tx)
    }

    pub(crate) fn remove_pending_dynamic_tool(
        &mut self,
        key: &str,
    ) -> Option<oneshot::Sender<DynamicToolResponse>> {
        self.pending_dynamic_tools.remove(key)
    }

    pub(crate) fn accept_mailbox_delivery_for_current_turn(&mut self) {
        self.set_mailbox_delivery_phase(MailboxDeliveryPhase::CurrentTurn);
    }

    pub(crate) fn accepts_mailbox_delivery_for_current_turn(&self) -> bool {
        self.mailbox_delivery_phase == MailboxDeliveryPhase::CurrentTurn
    }

    pub(crate) fn set_mailbox_delivery_phase(&mut self, phase: MailboxDeliveryPhase) {
        self.mailbox_delivery_phase = phase;
    }

    pub(crate) fn record_granted_permissions(&mut self, permissions: AdditionalPermissionProfile) {
        self.granted_permissions =
            merge_permission_profiles(self.granted_permissions.as_ref(), Some(&permissions));
    }

    pub(crate) fn granted_permissions(&self) -> Option<AdditionalPermissionProfile> {
        self.granted_permissions.clone()
    }

    pub(crate) fn enable_strict_auto_review(&mut self) {
        self.strict_auto_review_enabled = true;
    }

    pub(crate) fn strict_auto_review_enabled(&self) -> bool {
        self.strict_auto_review_enabled
    }
}

fn plan_is_completed(plan: &[PlanItemArg]) -> bool {
    !plan.is_empty() && plan.iter().all(|item| item.status == StepStatus::Completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_plan_update_appends_and_updates_open_items() {
        let mut state = TurnState::default();
        let created = state
            .apply_external_plan_update(
                Some("initial".to_string()),
                vec![ExternalPlanUpdateOperation::Append {
                    step: "draft patch".to_string(),
                    status: None,
                }],
            )
            .expect("append should work");
        assert!(created.created);
        assert!(created.updated);
        assert!(!created.completed);
        assert_eq!(created.current_plan.plan[0].status, StepStatus::Pending);

        let updated = state
            .apply_external_plan_update(
                None,
                vec![ExternalPlanUpdateOperation::Update {
                    index: 0,
                    step: Some("ship patch".to_string()),
                    status: Some(StepStatus::InProgress),
                }],
            )
            .expect("update should work");
        assert!(!updated.created);
        assert_eq!(
            updated.previous_plan.expect("previous plan")[0].step,
            "draft patch"
        );
        assert_eq!(updated.current_plan.plan[0].step, "ship patch");
        assert_eq!(updated.current_plan.plan[0].status, StepStatus::InProgress);
    }

    #[test]
    fn external_plan_update_rejects_completed_items_without_mutating() {
        let mut state = TurnState::default();
        state.apply_plan_snapshot(UpdatePlanArgs {
            explanation: None,
            plan: vec![PlanItemArg {
                step: "done".to_string(),
                status: StepStatus::Completed,
            }],
        });

        let error = state
            .apply_external_plan_update(
                None,
                vec![ExternalPlanUpdateOperation::Update {
                    index: 0,
                    step: Some("rewrite history".to_string()),
                    status: Some(StepStatus::Pending),
                }],
            )
            .expect_err("completed item cannot be changed");
        assert_eq!(error, "plan item index 0 is already completed");
        assert_eq!(
            state.current_plan.as_ref().expect("plan").plan[0].step,
            "done"
        );
        assert_eq!(
            state.current_plan.as_ref().expect("plan").plan[0].status,
            StepStatus::Completed
        );
    }

    #[test]
    fn completed_plan_transition_fires_once() {
        let mut state = TurnState::default();
        let first = state.apply_plan_snapshot(UpdatePlanArgs {
            explanation: None,
            plan: vec![PlanItemArg {
                step: "finish".to_string(),
                status: StepStatus::Completed,
            }],
        });
        assert!(first.completed);

        let second = state.apply_plan_snapshot(UpdatePlanArgs {
            explanation: None,
            plan: vec![PlanItemArg {
                step: "finish".to_string(),
                status: StepStatus::Completed,
            }],
        });
        assert!(!second.completed);
    }
}
