use super::types::{
    ActiveWindow, ActiveWindowSway, CompositorCommand, CompositorEvent, CompositorMonitor,
    CompositorService, CompositorState, CompositorWorkspace,
};
use crate::services::ServiceEvent;
use anyhow::{anyhow, Result};
use itertools::Itertools;
use std::env;
use swayipc_async::{Connection, EventType};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;

pub async fn execute_command(cmd: CompositorCommand) -> Result<()> {
    let mut conn = Connection::new().await?;

    let command = match cmd {
        CompositorCommand::FocusWorkspace(id) => format!("workspace number {id}"),
        CompositorCommand::FocusSpecialWorkspace(_) => {
            return Err(anyhow!("Special workspaces not supported on Sway"));
        }
        CompositorCommand::ToggleSpecialWorkspace(_) => {
            return Err(anyhow!("Special workspaces not supported on Sway"));
        }
        CompositorCommand::FocusMonitor(id) => {
            // Fix: get_outputs() is a method on Connection, not a free function
            let outputs = conn.get_outputs().await?;
            let name = outputs
                .get(id as usize)
                .ok_or_else(|| anyhow!("Output index {} out of range", id))?
                .name
                .clone();
            format!("focus output {name}")
        }
        CompositorCommand::ScrollWorkspace(dir) => {
            if dir > 0 {
                "workspace next"
            } else {
                "workspace prev"
            }
            .to_string()
        }
        CompositorCommand::NextLayout => "input toggle_keymap".to_string(),
        CompositorCommand::CustomDispatch(_, args) => format!("exec {args}"),
    };

    let outcomes = conn.run_command(&command).await?;
    for outcome in outcomes {
        if let Err(e) = outcome {
            // Fix: CommandError has no .message field; use Display impl directly
            return Err(anyhow!("{}", e));
        }
    }

    Ok(())
}

pub fn is_available() -> bool {
    // Fix: add `use std::env;` at the top
    env::var_os("SWAYSOCK").is_some()
}

pub async fn run_listener(
    tx: &broadcast::Sender<ServiceEvent<CompositorService>>,
) -> Result<()> {
    // Fix: conn must be mut since subscribe() takes &mut self
    let conn = Connection::new().await?;

    let subs = [
        EventType::Workspace,
        EventType::Window,
        EventType::Mode,
        EventType::Input,
        EventType::Output,
    ];
    let mut events = conn.subscribe(subs).await?;

    // Initial state fetch
    // Fix: fetch_full_state is now async
    let state = fetch_full_state().await?;
    let _ = tx.send(ServiceEvent::Update(CompositorEvent::StateChanged(
        Box::new(state),
    )));

    // Fix: StreamExt is now in scope so .next() works
    while let Some(event) = events.next().await {
        let _event = event?;
        let state = fetch_full_state().await?;
        let _ = tx.send(ServiceEvent::Update(CompositorEvent::StateChanged(
            Box::new(state),
        )));
    }

    Ok(())
}

// Fix: must be async — it calls async IPC methods
async fn fetch_full_state() -> Result<CompositorState> {
    let mut conn = Connection::new().await?;

    // Fix: all of these are async methods on Connection
    let workspaces = conn.get_workspaces().await?;
    let outputs = conn.get_outputs().await?;
    let tree = conn.get_tree().await?;
    let inputs = conn.get_inputs().await?;

    // Build output → active workspace num mapping
    let output_to_active_ws: std::collections::HashMap<_, _> = outputs
        .iter()
        .filter_map(|o| {
            o.current_workspace
                .as_ref()
                .and_then(|ws_name| {
                    workspaces
                        .iter()
                        .find(|w| w.name == *ws_name)
                        .map(|w| w.num)
                })
                .map(|ws_id| (o.name.clone(), ws_id))
        })
        .collect();

    // Sort outputs by name for stable ordering
    let outputs_sorted: Vec<_> = outputs
        .iter()
        .sorted_by_key(|o| o.name.clone())
        .collect();

    // Build monitors
    let monitors: Vec<CompositorMonitor> = outputs_sorted
        .iter()
        .enumerate()
        .map(|(i, o)| CompositorMonitor {
            id: i as i128,
            name: o.name.clone(),
            active_workspace_id: output_to_active_ws
                .get(&o.name)
                .copied()
                .unwrap_or(-1),
            special_workspace_id: -1,
        })
        .collect();

    // Build workspaces — renamed to avoid shadowing `workspaces` from get_workspaces()
    let mut workspace_list: Vec<CompositorWorkspace> = workspaces
        .iter()
        .sorted_by_key(|w| w.num)
        .map(|w| {
            CompositorWorkspace {
                id: w.num,
                index: w.num,
                name: w.name.clone(),
                monitor: w.output.clone(),
                monitor_id: None,
                windows: 0,
                is_special: false,
                has_urgent: w.urgent,
            }
        })
        .collect();

    let mut workspace_window_counts: std::collections::HashMap<i32, u16> =
        std::collections::HashMap::new();
    let mut active_window: Option<ActiveWindow> = None;

    // Fix: get_tree() returns a single Node, not a Vec — iterate directly
    find_window_info(&tree, &mut workspace_window_counts, &mut active_window);

    for ws in workspace_list.iter_mut() {
        if let Some(&count) = workspace_window_counts.get(&ws.id) {
            ws.windows = count;
        }
    }

    // Fix: removed the broken `?` inside a bool-returning closure.
    // Find the focused output, then look up its active workspace num.
    let active_workspace_id = outputs_sorted
        .iter()
        .find(|o| o.focused)
        .and_then(|o| output_to_active_ws.get(&o.name).copied());

    let keyboard_layout = inputs
        .iter()
        .find(|i| i.input_type == "keyboard")
        .and_then(|k| k.xkb_active_layout_name.clone())
        .unwrap_or_else(|| "Unknown".to_string());

    Ok(CompositorState {
        workspaces: workspace_list,
        monitors,
        active_workspace_id,
        active_window,
        keyboard_layout,
        submap: None,
    })
}

fn find_window_info(
    node: &swayipc_async::Node,
    workspace_counts: &mut std::collections::HashMap<i32, u16>,
    active_window: &mut Option<ActiveWindow>,
) {
    if node.node_type == swayipc_async::NodeType::Workspace {
        let mut count: u16 = 0;
        for child in &node.nodes {
            count += count_windows(child);
        }
        for child in &node.floating_nodes {
            count += count_windows(child);
        }
        if let Some(num) = node.num {
            workspace_counts.insert(num, count);
        }
    }

    if node.node_type == swayipc_async::NodeType::Con
        || node.node_type == swayipc_async::NodeType::FloatingCon
    {
        if node.focused {
            *active_window = Some(ActiveWindow::Sway(ActiveWindowSway {
                title: node.name.clone().unwrap_or_default(),
                class: node.app_id.clone().unwrap_or_default(),
                address: node.id.to_string(),
            }));
        }
    }

    for child in &node.nodes {
        find_window_info(child, workspace_counts, active_window);
    }
    for child in &node.floating_nodes {
        find_window_info(child, workspace_counts, active_window);
    }
}

fn count_windows(node: &swayipc_async::Node) -> u16 {
    let mut count: u16 = 0;

    for child in &node.nodes {
        count += count_windows(child);
    }
    for child in &node.floating_nodes {
        count += count_windows(child);
    }

    if node.nodes.is_empty() && node.floating_nodes.is_empty() {
        count = 1;
    }

    count
}
