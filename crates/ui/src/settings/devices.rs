//! Settings → Devices (feature-inventory §1.5): the device registry — name,
//! platform, last-seen, an Online/Offline badge, a "This device" badge, click-to-copy id,
//! a Rename dialog (Mutate renameDevice), and a Forget dialog for offline devices
//! (Mutate forgetDevice).

use chrono::{DateTime, Utc};
use gpui::{
    AnyElement, ClipboardItem, Context, Entity, SharedString, Subscription, Task, Window, div,
    prelude::*, px,
};
use std::time::Duration;

use zeron_proto::WorkspaceScope;
use zeron_rpc::methods;

use crate::composer::{ComposerInput, ComposerInputEvent};
use crate::popover;
use crate::settings::widgets;
use crate::state::AppState;
use crate::theme::Theme;

/// A device that pinged within this window shows a presence dot (engines
/// heartbeat every 15s; 70s tolerates a couple of missed beats).
pub const DEVICE_ONLINE_WINDOW_SECS: i64 = 70;

/// Presence: last-seen within the online window (future timestamps count). Pure.
pub fn device_online(last_seen: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    last_seen
        .is_some_and(|at| now.signed_duration_since(at).num_seconds() <= DEVICE_ONLINE_WINDOW_SECS)
}

/// Compact last-seen line. Pure.
pub fn format_last_seen(last_seen: Option<DateTime<Utc>>, now: DateTime<Utc>) -> String {
    let Some(at) = last_seen else {
        return "never seen".to_string();
    };
    let secs = now.signed_duration_since(at).num_seconds();
    if secs < 60 {
        "just now".to_string()
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

/// Scope-aware copy: a local registry describes only the active local
/// workspace and must not imply that account device metadata is already live.
pub fn devices_subtitle(scope: Option<WorkspaceScope>) -> &'static str {
    match scope {
        Some(WorkspaceScope::Local) => "Devices in this local workspace.",
        Some(WorkspaceScope::Synced) => "Devices synced to this workspace.",
        Some(WorkspaceScope::Development) | None => "Devices in this workspace.",
    }
}

/// "Forget…" sits on another device's row while it is offline, and only when
/// the local engine understands `forgetDevice`. Pure.
pub fn forget_offered(is_local: bool, online: bool, engine_supports: bool) -> bool {
    !is_local && !online && engine_supports
}

fn count_noun(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// Why `name` can't be forgotten yet, given its `(projects, sessions)`
/// ([`zeron_proto::device_holdings`]); `None` when nothing pins it. Pure.
pub fn forget_refusal(name: &str, (projects, sessions): (usize, usize)) -> Option<String> {
    let held: Vec<String> = [(projects, "project"), (sessions, "session")]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, noun)| count_noun(count, noun))
        .collect();
    (!held.is_empty()).then(|| {
        format!(
            "“{name}” still has {}. Remove them first, then forget the device.",
            held.join(" and ")
        )
    })
}

struct RenameDialog {
    device_id: String,
    input: Entity<ComposerInput>,
    _events: Subscription,
}

pub struct DevicesPage {
    state: Entity<AppState>,
    scroll: widgets::PageScroll,
    rename: Option<RenameDialog>,
    /// Device id the Forget dialog is open for.
    forget: Option<String>,
    /// Device id whose id-chip shows "Copied" right now.
    copied: Option<String>,
    error: Option<SharedString>,
    task: Option<Task<()>>,
    copy_task: Option<Task<()>>,
    _observe: Subscription,
}

impl DevicesPage {
    pub fn new(state: Entity<AppState>, cx: &mut Context<Self>) -> Self {
        let observe = cx.observe(&state, |_, _, cx| cx.notify());
        Self {
            state,
            scroll: widgets::PageScroll::default(),
            rename: None,
            forget: None,
            copied: None,
            error: None,
            task: None,
            copy_task: None,
            _observe: observe,
        }
    }

    pub(crate) fn open_rename(
        &mut self,
        device_id: String,
        current: String,
        cx: &mut Context<Self>,
    ) {
        let input = cx.new(|cx| ComposerInput::new("Device name", cx));
        input.update(cx, |input, cx| input.set_text(current, cx));
        let events = cx.subscribe(&input, |this: &mut Self, _, event, cx| {
            if matches!(event, ComposerInputEvent::Submitted) {
                this.submit_rename(cx);
            }
        });
        self.rename = Some(RenameDialog {
            device_id,
            input,
            _events: events,
        });
        cx.notify();
    }

    pub(crate) fn open_forget(&mut self, device_id: String, cx: &mut Context<Self>) {
        self.forget = Some(device_id);
        cx.notify();
    }

    /// Escape that reached Settings unclaimed closes an open dialog first,
    /// so it never closes Settings under the dialog. Returns whether it did.
    pub(crate) fn dismiss_on_escape(&mut self, cx: &mut Context<Self>) -> bool {
        if self.rename.take().is_none() && self.forget.take().is_none() {
            return false;
        }
        cx.notify();
        true
    }

    fn submit_rename(&mut self, cx: &mut Context<Self>) {
        let Some(dialog) = self.rename.take() else {
            return;
        };
        let name = dialog.input.read(cx).text().trim().to_string();
        if name.is_empty() {
            cx.notify();
            return;
        }
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        let params = serde_json::json!({
            "op": "renameDevice",
            "deviceId": dialog.device_id,
            "name": name,
        });
        self.task = Some(cx.spawn(async move |this, cx| {
            let result = engine.client().call(methods::MUTATE, params).await;
            this.update(cx, |page, cx| {
                if let Err(err) = result {
                    page.error = Some(format!("Rename failed: {err}").into());
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn forget_device(&mut self, device_id: String, cx: &mut Context<Self>) {
        self.forget = None;
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        let params = serde_json::json!({ "op": "forgetDevice", "deviceId": device_id });
        self.task = Some(cx.spawn(async move |this, cx| {
            let result = engine.client().call(methods::MUTATE, params).await;
            this.update(cx, |page, cx| {
                if let Err(err) = result {
                    page.error = Some(format!("Forget failed: {err}").into());
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn copy_id(&mut self, device_id: String, cx: &mut Context<Self>) {
        cx.write_to_clipboard(ClipboardItem::new_string(device_id.clone()));
        self.copied = Some(device_id);
        self.copy_task = Some(cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_millis(1500))
                .await;
            this.update(cx, |page, cx| {
                page.copied = None;
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn render_rename_dialog(
        &mut self,
        viewport: gpui::Size<gpui::Pixels>,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let theme = Theme::of(cx).for_popup();
        let dialog = self.rename.as_ref()?;
        let input = dialog.input.clone();
        let card = popover::dialog_card(&theme)
            .id("rename-device-card")
            .role(gpui::Role::Dialog)
            .aria_label("Rename device")
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, _, cx| {
                if event.keystroke.key == "escape" {
                    this.rename = None;
                    cx.notify();
                    cx.stop_propagation();
                }
            }))
            .child(popover::dialog_title(&theme, "Rename device"))
            .child(
                div()
                    .mt(px(12.0))
                    .child(popover::dialog_field(input.into_any_element())),
            )
            .child(
                div()
                    .mt(px(16.0))
                    .flex()
                    .flex_row()
                    .justify_end()
                    .gap(px(8.0))
                    .child(
                        widgets::text_action(&theme, widgets::ActionTone::Quiet, "Cancel")
                            .id("rename-cancel")
                            .tab_index(0)
                            .role(gpui::Role::Button)
                            .focus_visible(|s| s.border_2().border_color(theme.accent).opacity(1.0))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.rename = None;
                                cx.notify();
                            })),
                    )
                    .child(
                        widgets::text_action(&theme, widgets::ActionTone::Solid, "Rename")
                            .id("rename-save")
                            .tab_index(0)
                            .role(gpui::Role::Button)
                            .focus_visible(|s| s.border_2().border_color(theme.accent).opacity(1.0))
                            .on_click(cx.listener(|this, _, _, cx| this.submit_rename(cx))),
                    ),
            )
            .into_any_element();
        Some(popover::modal("rename-device-dialog", viewport, card))
    }

    /// The project "Remove…" confirm, for a device. While the device still
    /// hosts projects or sessions it explains what to remove instead.
    fn render_forget_dialog(
        &mut self,
        viewport: gpui::Size<gpui::Pixels>,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let theme = Theme::of(cx).for_popup();
        let device_id = self.forget.clone()?;
        let (name, holdings) = {
            let state = self.state.read(cx);
            (
                state.device_name(&device_id)?.to_string(),
                zeron_proto::device_holdings(&device_id, &state.spaces, &state.chats),
            )
        };
        let refusal = forget_refusal(&name, holdings);
        let (title, copy) = match &refusal {
            Some(refusal) => ("Can’t forget device yet", refusal.clone()),
            None => (
                "Forget device?",
                format!(
                    "Forgetting “{name}” removes it from Devices on all your devices. If it runs Zeron again, it reappears."
                ),
            ),
        };
        let buttons = div()
            .mt(px(16.0))
            .flex()
            .flex_row()
            .justify_end()
            .gap(px(8.0))
            .child(
                popover::btn_ghost(
                    &theme,
                    if refusal.is_some() { "Close" } else { "Cancel" },
                    "forget-device-cancel",
                )
                .id("forget-device-cancel")
                .on_click(cx.listener(|this, _, _, cx| {
                    this.forget = None;
                    cx.notify();
                })),
            )
            .when(refusal.is_none(), |el| {
                el.child(
                    popover::btn_danger(&theme, "Forget")
                        .id("forget-device-confirm")
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.forget_device(device_id.clone(), cx)
                        })),
                )
            });
        let card = popover::dialog_card(&theme)
            .id("forget-device-card")
            .role(gpui::Role::Dialog)
            .aria_label(title)
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, _, cx| {
                if event.keystroke.key == "escape" {
                    this.forget = None;
                    cx.notify();
                    cx.stop_propagation();
                }
            }))
            .child(popover::dialog_title(&theme, title))
            .child(div().mt(px(6.0)).child(popover::dialog_body(&theme, copy)))
            .child(buttons)
            .into_any_element();
        Some(popover::modal("forget-device-dialog", viewport, card))
    }

    fn on_scroll_hovered(&mut self, hovered: &bool, _: &mut Window, cx: &mut Context<Self>) {
        if self.scroll.set_list_hovered(*hovered) {
            cx.notify();
        }
    }
}

impl popover::ScrollRailHost for DevicesPage {
    fn rail_bar(&mut self) -> &mut popover::MenuScrollbarState {
        self.scroll.rail_bar()
    }

    fn rail_scroll(&self) -> Option<gpui::ScrollHandle> {
        self.scroll.rail_scroll()
    }
}

/// Human platform label (zeron settings.devices.tsx `platformLabel`).
pub fn platform_label(platform: &str) -> &str {
    match platform {
        "macos" | "darwin" => "macOS",
        "linux" => "Linux",
        "windows" => "Windows",
        "web" => "Web",
        "ios" => "iOS",
        "android" => "Android",
        other => other,
    }
}

/// Short device id for the click-to-copy chip (`abcd1234…wxyz`).
pub fn short_id(id: &str) -> String {
    if id.len() > 12 {
        format!("{}…{}", &id[..8], &id[id.len() - 4..])
    } else {
        id.to_string()
    }
}

impl Render for DevicesPage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        crate::toast::take_error(&mut self.error, cx);
        let theme = Theme::of(cx).for_settings_surface();
        let now = Utc::now();
        let (devices, local_id, workspace_scope, can_forget) = {
            let state = self.state.read(cx);
            (
                state.devices.clone(),
                state.local_device_id.clone(),
                state.workspace_scope,
                state.engine().is_some_and(|engine| {
                    engine
                        .engine_info()
                        .supports(zeron_proto::capabilities::FORGET_DEVICE_V1)
                }),
            )
        };
        let copied = self.copied.clone();
        let dialog = self
            .render_rename_dialog(window.viewport_size(), cx)
            .or_else(|| self.render_forget_dialog(window.viewport_size(), cx));
        // Split into this device and the rest; each renders as rows in one
        // block, like every other settings page.
        let (local, others): (Vec<_>, Vec<_>) = devices
            .into_iter()
            .enumerate()
            .partition(|(_, device)| local_id.as_deref() == Some(device.id.as_str()));
        let device_row = |ix: usize, device: zeron_proto::Device, first: bool| {
            let online = device_online(device.last_seen_at, now);
            let is_local = local_id.as_deref() == Some(device.id.as_str());
            let id_copied = copied.as_deref() == Some(device.id.as_str());
            let copy_id = device.id.clone();
            let rename_id = device.id.clone();
            let rename_name = device.name.clone();
            let forget_id = device.id.clone();
            let offer_forget = forget_offered(is_local, online, can_forget);
            let mut meta: Vec<AnyElement> = vec![
                div()
                    .child(SharedString::from(
                        platform_label(&device.platform).to_string(),
                    ))
                    .into_any_element(),
            ];
            if let Some(version) = device.version.as_deref().filter(|v| !v.is_empty()) {
                meta.push(
                    div()
                        .child(SharedString::from(format!("v{version}")))
                        .into_any_element(),
                );
            }
            // Presence only says something about other devices.
            if !is_local {
                meta.push(if online {
                    div()
                        .text_color(theme.success_muted)
                        .child(SharedString::from("Online"))
                        .into_any_element()
                } else {
                    div()
                        .child(SharedString::from(format!(
                            "Last seen {}",
                            format_last_seen(device.last_seen_at, now)
                        )))
                        .into_any_element()
                });
            }
            widgets::card_row(&theme, first)
                .child(
                    div()
                        .flex_1()
                        .min_w(px(160.0))
                        .child(widgets::row_title(&theme, device.name.clone()))
                        .child(widgets::meta_line(&theme, meta)),
                )
                .child(
                    div()
                        .flex_none()
                        .flex()
                        .items_center()
                        .gap(px(4.0))
                        .child(
                            widgets::text_action(
                                &theme,
                                widgets::ActionTone::Quiet,
                                if id_copied { "Copied" } else { "Copy ID" },
                            )
                            .id(("device-id", ix))
                            .tab_index(0)
                            .role(gpui::Role::Button)
                            .aria_label(format!("Copy device ID {}", device.id))
                            .focus_visible(|s| s.border_2().border_color(theme.accent))
                            .on_click(cx.listener(
                                move |this, _, _, cx| {
                                    this.copy_id(copy_id.clone(), cx);
                                },
                            )),
                        )
                        .when(offer_forget, |el| {
                            el.child(
                                widgets::text_action(&theme, widgets::ActionTone::Quiet, "Forget…")
                                    .id(("device-forget", ix))
                                    .tab_index(0)
                                    .role(gpui::Role::Button)
                                    .aria_label(format!("Forget {}", device.name))
                                    .focus_visible(|s| s.border_2().border_color(theme.accent))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.open_forget(forget_id.clone(), cx);
                                    })),
                            )
                        })
                        .child(
                            widgets::text_action(&theme, widgets::ActionTone::Filled, "Rename")
                                .id(("device-rename", ix))
                                .tab_index(0)
                                .role(gpui::Role::Button)
                                .aria_label(format!("Rename {}", device.name))
                                .focus_visible(|s| s.border_2().border_color(theme.accent))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.open_rename(rename_id.clone(), rename_name.clone(), cx);
                                })),
                        ),
                )
                .into_any_element()
        };
        let local_block = (!local.is_empty()).then(|| {
            let mut block = widgets::section_card(&theme).mt(px(8.0));
            for (n, (ix, device)) in local.into_iter().enumerate() {
                block = block.child(device_row(ix, device, n == 0));
            }
            block
        });
        let others_block = if others.is_empty() {
            widgets::section_card(&theme).mt(px(8.0)).child(
                widgets::card_row(&theme, true).child(
                    div()
                        .text_size(crate::typography::ui_rems(13.0))
                        .text_color(theme.text_muted)
                        .child(SharedString::from(
                            "Sign in on another device to see it here.",
                        )),
                ),
            )
        } else {
            let mut block = widgets::section_card(&theme).mt(px(8.0));
            for (n, (ix, device)) in others.into_iter().enumerate() {
                block = block.child(device_row(ix, device, n == 0));
            }
            block
        };
        let card = div()
            .flex()
            .flex_col()
            .when_some(local_block, |el, block| {
                el.child(widgets::section_label(&theme, "This device").mt(px(28.0)))
                    .child(block)
            })
            // A local-only workspace never has other devices to list.
            .when(workspace_scope != Some(WorkspaceScope::Local), |el| {
                el.child(widgets::section_label(&theme, "Other devices").mt(px(28.0)))
                    .child(others_block)
            });

        let scrollbar = popover::rail(self, "devices-page-scrollbar", &theme, cx);
        div()
            .id("devices-page-host")
            .relative()
            .size_full()
            .on_hover(cx.listener(Self::on_scroll_hovered))
            .child(
                crate::edge_fade::edge_faded(
                    16.0,
                    true,
                    true,
                    div()
                        .id("devices-page")
                        .size_full()
                        .overflow_y_scroll()
                        .track_scroll(&self.scroll.scroll)
                        .child(
                            widgets::page_column()
                                .child(widgets::page_header(&theme, "Devices", None))
                                .child(widgets::page_subtitle(
                                    &theme,
                                    devices_subtitle(workspace_scope),
                                ))
                                .child(card),
                        ),
                )
                .fade_overflow_y(&self.scroll.scroll),
            )
            .children(scrollbar)
            .when_some(dialog, |el, dialog| el.child(dialog))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn presence_window() {
        let now = Utc::now();
        assert!(device_online(Some(now - TimeDelta::seconds(10)), now));
        assert!(device_online(Some(now - TimeDelta::seconds(70)), now));
        assert!(!device_online(Some(now - TimeDelta::seconds(71)), now));
        assert!(!device_online(None, now));
        // Clock skew (future) counts as online.
        assert!(device_online(Some(now + TimeDelta::seconds(30)), now));
    }

    #[test]
    fn last_seen_formatting() {
        let now = Utc::now();
        assert_eq!(format_last_seen(None, now), "never seen");
        assert_eq!(
            format_last_seen(Some(now - TimeDelta::seconds(30)), now),
            "just now"
        );
        assert_eq!(
            format_last_seen(Some(now - TimeDelta::minutes(5)), now),
            "5m ago"
        );
        assert_eq!(
            format_last_seen(Some(now - TimeDelta::hours(3)), now),
            "3h ago"
        );
        assert_eq!(
            format_last_seen(Some(now - TimeDelta::days(2)), now),
            "2d ago"
        );
    }

    #[test]
    fn forget_is_offered_only_for_offline_peers_on_a_capable_engine() {
        assert!(forget_offered(false, false, true));
        // Never for this device, even when the engine supports it.
        assert!(!forget_offered(true, false, true));
        // Never while the device is online.
        assert!(!forget_offered(false, true, true));
        // An older local engine has no forgetDevice op.
        assert!(!forget_offered(false, false, false));
    }

    #[test]
    fn forget_refuses_with_counts_while_the_device_hosts_anything() {
        let space = |id: &str, device: &str| zeron_proto::Space {
            id: id.into(),
            device_id: device.into(),
            path: format!("/tmp/{id}"),
            name: None,
            git_detected: false,
            git_checked_at: None,
            checkout_id: None,
            repository_id: None,
            created_at: Utc::now(),
        };
        let chat = |id: &str, device: &str| zeron_proto::Chat {
            id: id.into(),
            device_id: device.into(),
            title: None,
            archived: false,
            cwd: None,
            branch: None,
            checkout_id: None,
            source_context: None,
            config: None,
            last_message_preview: None,
            last_message_at: None,
            created_at: Utc::now(),
            harness_session_id: None,
            harness_session_cwd: None,
            parent_chat_id: None,
            space_id: None,
            last_seen_at: None,
            room_gen: None,
        };
        let spaces = vec![
            space("sp-1", "old-laptop"),
            space("sp-2", "old-laptop"),
            space("sp-3", "desk"),
        ];
        let mut archived = chat("c-archived", "old-laptop");
        archived.archived = true;
        let mut side = chat("c-side", "old-laptop");
        side.parent_chat_id = Some("c-1".into());
        let chats = vec![
            chat("c-1", "old-laptop"),
            archived,
            side,
            chat("c-2", "desk"),
        ];

        let holdings = zeron_proto::device_holdings("old-laptop", &spaces, &chats);
        assert_eq!(holdings, (2, 1));
        assert_eq!(
            forget_refusal("old-laptop", holdings).as_deref(),
            Some(
                "“old-laptop” still has 2 projects and 1 session. Remove them first, then forget the device."
            )
        );
        assert_eq!(
            forget_refusal("old-laptop", (0, 3)).as_deref(),
            Some("“old-laptop” still has 3 sessions. Remove them first, then forget the device.")
        );
        assert_eq!(
            zeron_proto::device_holdings("old-laptop", &[], &chats[1..3]),
            (0, 0)
        );
        assert_eq!(forget_refusal("old-laptop", (0, 0)), None);
    }

    #[test]
    fn local_subtitle_does_not_claim_synced_metadata() {
        let copy = devices_subtitle(Some(WorkspaceScope::Local));
        assert!(copy.contains("local workspace"));
        assert!(!copy.contains("synced"));
    }
}
