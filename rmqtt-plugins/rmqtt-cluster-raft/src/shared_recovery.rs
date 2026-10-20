//! Maintenance-only shared-subscription selection. This does not authorize
//! replay: the caller must first fence or prove the previous attempt unstarted.

use std::collections::HashSet;

use rmqtt::{
    context::ServerContext,
    router::Router,
    types::{ClientId, From, Id, NodeId, SharedGroup, SubRelation, SubscriptionOptions, TopicFilter},
};

use super::router::{ClientStatus, ClusterRouter};

fn shared_group(relation: &SubRelation) -> Option<&SharedGroup> {
    match (relation.4.as_ref(), relation.2.shared_group()) {
        (Some((group, _, _)), Some(option_group)) if group != option_group => None,
        (Some((group, _, _)), _) => Some(group),
        (None, option_group) => option_group,
    }
}

fn owner_is_ready(route_id: &Id, status: Option<&ClientStatus>) -> bool {
    status.is_some_and(|status| status.online && !status.handshaking && status.id == *route_id)
}

fn candidate_allowed(
    group: &SharedGroup,
    route_id: &Id,
    opts: &SubscriptionOptions,
    status: Option<&ClientStatus>,
    from: &From,
    avoid_client: &str,
    excluded_nodes: &HashSet<NodeId>,
) -> bool {
    route_id.client_id != avoid_client
        && !excluded_nodes.contains(&route_id.node_id)
        && opts.shared_group() == Some(group)
        && !(opts.no_local() == Some(true) && from.id == *route_id)
        && owner_is_ready(route_id, status)
}

/// Diagnostic counts cover all routes for the exact filter. Rejection counts
/// deliberately overlap: one route can be both excluded and offline.
// Debug emits every field in the rate-limited diagnostic log.
#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct SharedCandidateDiagnostics {
    pub group_state: &'static str,
    pub group: Option<SharedGroup>,
    pub filter_routes: usize,
    pub same_group: usize,
    pub eligible: usize,
    pub old_client: usize,
    pub excluded_node: usize,
    pub no_local: usize,
    pub missing_status: usize,
    pub offline: usize,
    pub handshaking: usize,
    pub owner_mismatch: usize,
    pub samples: Vec<SharedCandidateSample>,
}

// Debug emits every sample field; samples are not used to select a consumer.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SharedCandidateSample {
    pub client_id: ClientId,
    pub node_id: NodeId,
    pub same_group: bool,
    pub online: Option<bool>,
    pub handshaking: Option<bool>,
    pub owner_matches: Option<bool>,
    pub old_client: bool,
    pub excluded_node: bool,
    pub no_local: bool,
    pub eligible: bool,
}

impl SharedCandidateDiagnostics {
    fn observe_candidate(
        &mut self,
        route_id: &Id,
        opts: &SubscriptionOptions,
        status: Option<&ClientStatus>,
        from: &From,
        avoid_client: &str,
        excluded_nodes: &HashSet<NodeId>,
    ) {
        let same_group = self.group.as_ref().is_some_and(|group| opts.shared_group() == Some(group));
        let old_client = route_id.client_id == avoid_client;
        let excluded_node = excluded_nodes.contains(&route_id.node_id);
        let no_local = opts.no_local() == Some(true) && from.id == *route_id;
        let eligible = self.group.as_ref().is_some_and(|group| {
            candidate_allowed(group, route_id, opts, status, from, avoid_client, excluded_nodes)
        });
        self.filter_routes += 1;
        self.same_group += usize::from(same_group);
        self.eligible += usize::from(eligible);
        self.old_client += usize::from(old_client);
        self.excluded_node += usize::from(excluded_node);
        self.no_local += usize::from(no_local);
        self.missing_status += usize::from(status.is_none());
        self.offline += usize::from(status.is_some_and(|status| !status.online));
        self.handshaking += usize::from(status.is_some_and(|status| status.handshaking));
        self.owner_mismatch += usize::from(status.is_some_and(|status| status.id != *route_id));
        if self.samples.len() < 6 {
            self.samples.push(SharedCandidateSample {
                client_id: route_id.client_id.clone(),
                node_id: route_id.node_id,
                same_group,
                online: status.map(|status| status.online),
                handshaking: status.map(|status| status.handshaking),
                owner_matches: status.map(|status| status.id == *route_id),
                old_client,
                excluded_node,
                no_local,
                eligible,
            });
        }
    }
}

/// A bounded sample and local, non-atomic snapshot for rate-limited maintenance
/// logs only. It performs no RPC, chooser invocation, or state mutation and is
/// never a replay fence or proof that a previous delivery has not started.
pub(crate) fn shared_candidate_diagnostics(
    router: &ClusterRouter,
    relation: &SubRelation,
    from: &From,
    avoid_client: &str,
    excluded_nodes: &HashSet<NodeId>,
) -> SharedCandidateDiagnostics {
    let group = shared_group(relation).cloned();
    let group_state = match (relation.4.as_ref(), relation.2.shared_group()) {
        (Some((group, _, _)), Some(option_group)) if group != option_group => "conflicting",
        (None, None) => "missing",
        _ => "valid",
    };
    let mut diagnostics = SharedCandidateDiagnostics { group_state, group, ..Default::default() };
    let routes = router
        .relations()
        .get(&relation.0)
        .map(|routes| routes.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    for (route_id, opts) in routes {
        let status = router.status(&route_id.client_id);
        diagnostics.observe_candidate(&route_id, &opts, status.as_ref(), from, avoid_client, excluded_nodes);
    }
    diagnostics
}

/// Local routing snapshot only. A true result is not proof that a previous
/// delivery has not started, and must never be used as a replay/cancel fence.
pub(crate) fn is_shared_relation_current(router: &ClusterRouter, relation: &SubRelation) -> bool {
    let Some(group) = shared_group(relation) else {
        return false;
    };
    let route = router.relations().get(&relation.0).and_then(|routes| routes.get(&relation.1).cloned());
    let Some((route_id, opts)) = route else {
        return false;
    };
    opts.shared_group() == Some(group)
        && opts == relation.2
        && owner_is_ready(&route_id, router.status(&relation.1).as_ref())
}

/// Recover the original consumer's real subscription options after the caller
/// has fenced an unstarted delivery. Recovery contexts contain the source QoS,
/// not subscription options: synthesizing options from that QoS would either
/// make a live consumer look stale or lose its MQTT 5 delivery semantics.
///
/// This is a maintenance-only local snapshot, not a replay/cancellation fence.
/// A missing or changed route must remain an unbound recovery intent until the
/// caller can select an eligible member; it must not pass the strict options
/// comparison in `is_shared_relation_current` using fabricated options.
pub(crate) fn bind_current_shared_relation(
    router: &ClusterRouter,
    topic_filter: &TopicFilter,
    target_id: &Id,
    group: &SharedGroup,
    from: &From,
) -> Option<SubRelation> {
    let route = router.relations().get(topic_filter)?.get(&target_id.client_id).cloned()?;
    bind_shared_route(
        topic_filter,
        target_id,
        group,
        from,
        route,
        router.status(&target_id.client_id).as_ref(),
    )
}

fn bind_shared_route(
    topic_filter: &TopicFilter,
    target_id: &Id,
    group: &SharedGroup,
    from: &From,
    (route_id, opts): (Id, SubscriptionOptions),
    status: Option<&ClientStatus>,
) -> Option<SubRelation> {
    if route_id != *target_id
        || opts.shared_group() != Some(group)
        || !owner_is_ready(&route_id, status)
        || (opts.no_local() == Some(true) && from.id == route_id)
    {
        return None;
    }
    let sub_ids = opts.subscription_identifier().map(|id| vec![id]);
    Some((
        topic_filter.clone(),
        route_id.client_id.clone(),
        opts,
        sub_ids,
        Some((group.clone(), true, vec![route_id.client_id.clone()])),
    ))
}

/// Observation of the existing selection attempt; it does not authorize replay.
#[derive(Debug, Default)]
pub(crate) struct SharedSelectionDiagnostic {
    pub outcome: &'static str,
    pub candidates: usize,
    pub selected_index: Option<usize>,
    pub selected_client: Option<ClientId>,
    pub selected_node: Option<NodeId>,
}

/// Choose only another online member of the exact original group and filter.
/// All DashMap guards are released before awaiting the configured chooser.
pub(crate) async fn select_shared_replacement(
    scx: &ServerContext,
    router: &ClusterRouter,
    relation: &SubRelation,
    from: &From,
    avoid_client: &str,
    excluded_nodes: &HashSet<NodeId>,
    diagnostic: &mut SharedSelectionDiagnostic,
) -> Option<(NodeId, SubRelation)> {
    *diagnostic = SharedSelectionDiagnostic { outcome: "starting", ..Default::default() };
    let Some(group) = shared_group(relation).cloned() else {
        diagnostic.outcome = "invalid_group";
        return None;
    };
    let routes = router
        .relations()
        .get(&relation.0)
        .map(|routes| routes.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let candidates = routes
        .into_iter()
        .filter(|(route_id, opts)| {
            candidate_allowed(
                &group,
                route_id,
                opts,
                router.status(&route_id.client_id).as_ref(),
                from,
                avoid_client,
                excluded_nodes,
            )
        })
        .collect::<Vec<_>>();
    diagnostic.candidates = candidates.len();
    let choices = candidates
        .iter()
        .map(|(route_id, opts)| {
            (
                route_id.node_id,
                route_id.client_id.clone(),
                opts.clone(),
                opts.subscription_identifier().map(|id| vec![id]),
                Some(true),
            )
        })
        .collect::<Vec<_>>();
    let Some((index, online)) = scx.extends.shared_subscription().await.choice(scx, &choices).await else {
        diagnostic.outcome = "chooser_none";
        return None;
    };
    diagnostic.selected_index = Some(index);
    if !online {
        diagnostic.outcome = "chooser_offline";
        return None;
    }
    let Some((route_id, opts)) = candidates.get(index) else {
        diagnostic.outcome = "chooser_index_out_of_range";
        return None;
    };
    diagnostic.selected_client = Some(route_id.client_id.clone());
    diagnostic.selected_node = Some(route_id.node_id);
    // Choice can await or be supplied by another plugin. Recheck the selected
    // route and owner generation rather than using the earlier snapshot.
    let current =
        router.relations().get(&relation.0).and_then(|routes| routes.get(&route_id.client_id).cloned());
    let Some((current_id, current_opts)) = current else {
        diagnostic.outcome = "selected_route_removed";
        return None;
    };
    if current_id != *route_id
        || current_opts != *opts
        || !candidate_allowed(
            &group,
            &current_id,
            &current_opts,
            router.status(&current_id.client_id).as_ref(),
            from,
            avoid_client,
            excluded_nodes,
        )
    {
        diagnostic.outcome = "selected_route_or_owner_changed";
        return None;
    }
    let sub_ids = current_opts.subscription_identifier().map(|id| vec![id]);
    let group_clients = candidates.iter().map(|(id, _)| id.client_id.clone()).collect();
    diagnostic.outcome = "selected";
    Some((
        current_id.node_id,
        (
            relation.0.clone(),
            current_id.client_id.clone(),
            current_opts,
            sub_ids,
            Some((group, true, group_clients)),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmqtt::types::QoS;

    fn options(group: Option<&str>, qos: QoS, sub_id: u32, no_local: bool) -> SubscriptionOptions {
        serde_json::from_value(serde_json::json!({
            "V5": {
                "qos": qos.value(), "shared_group": group, "no_local": no_local,
                "retain_as_published": false, "retain_handling": 0, "id": sub_id,
                "limit_subs": null
            }
        }))
        .unwrap()
    }

    fn status(id: Id, online: bool, handshaking: bool) -> ClientStatus {
        ClientStatus { id, online, handshaking, handshak_duration: 0 }
    }

    #[test]
    fn recovery_binding_keeps_v3_subscription_qos_instead_of_source_qos() {
        let group = SharedGroup::from_static("workers");
        let filter = "sensor/#".into();
        let id = Id::from(2, "consumer".into());
        let publisher = From::from_custom(Id::from(1, "publisher".into()));
        let opts: SubscriptionOptions = serde_json::from_value(serde_json::json!({
            "V3": { "qos": 1, "shared_group": "workers", "limit_subs": null }
        }))
        .unwrap();
        let ready = status(id.clone(), true, false);
        let mut fabricated = SubscriptionOptions::default();
        fabricated.set_qos(QoS::ExactlyOnce);
        assert_ne!(opts, fabricated);

        let bound =
            bind_shared_route(&filter, &id, &group, &publisher, (id.clone(), opts.clone()), Some(&ready))
                .unwrap();
        assert_eq!(bound.0, filter);
        assert_eq!(bound.1, id.client_id);
        assert_eq!(bound.2, opts);
        assert_eq!(bound.2.qos(), QoS::AtLeastOnce);
        assert!(bound.3.is_none());
        assert_eq!(shared_group(&bound), Some(&group));
    }

    #[test]
    fn recovery_binding_keeps_all_v5_options_and_subscription_identifier() {
        let group = SharedGroup::from_static("workers");
        let filter = "sensor/#".into();
        let id = Id::from(2, "consumer".into());
        let publisher = From::from_custom(Id::from(1, "publisher".into()));
        let opts: SubscriptionOptions = serde_json::from_value(serde_json::json!({
            "V5": {
                "qos": 1, "shared_group": "workers", "no_local": true,
                "retain_as_published": true, "retain_handling": 2, "id": 53,
                "limit_subs": null
            }
        }))
        .unwrap();
        let ready = status(id.clone(), true, false);
        let bound =
            bind_shared_route(&filter, &id, &group, &publisher, (id.clone(), opts.clone()), Some(&ready))
                .unwrap();
        assert_eq!(bound.2, opts);
        assert_eq!(bound.2.no_local(), Some(true));
        assert_eq!(bound.2.retain_as_published(), Some(true));
        assert_eq!(bound.2.retain_handling(), opts.retain_handling());
        assert_eq!(bound.3.as_ref().unwrap()[0].get(), 53);
        assert_eq!(shared_group(&bound), Some(&group));

        assert!(bind_shared_route(
            &filter,
            &id,
            &group,
            &From::from_custom(id.clone()),
            (id.clone(), opts),
            Some(&ready),
        )
        .is_none());
    }

    #[test]
    fn recovery_binding_requires_exact_group_route_and_ready_owner_generation() {
        let group = SharedGroup::from_static("workers");
        let filter = "sensor/#".into();
        let id = Id::from(2, "consumer".into());
        let publisher = From::from_custom(Id::from(1, "publisher".into()));
        let opts = options(Some("workers"), QoS::AtLeastOnce, 7, false);
        let ready = status(id.clone(), true, false);
        let bind = |route_id: Id, route_opts: SubscriptionOptions, owner: Option<&ClientStatus>| {
            bind_shared_route(&filter, &id, &group, &publisher, (route_id, route_opts), owner)
        };
        assert!(bind(id.clone(), opts.clone(), Some(&ready)).is_some());
        assert!(bind(id.clone(), options(Some("other"), QoS::AtLeastOnce, 7, false), Some(&ready)).is_none());
        assert!(bind(id.clone(), options(None, QoS::AtLeastOnce, 7, false), Some(&ready)).is_none());
        assert!(bind(id.relocated_to(3), opts.clone(), Some(&ready)).is_none());
        assert!(bind(id.clone(), opts.clone(), None).is_none());
        assert!(bind(id.clone(), opts.clone(), Some(&status(id.clone(), false, false))).is_none());
        assert!(bind(id.clone(), opts.clone(), Some(&status(id.clone(), true, true))).is_none());
        assert!(bind(id.clone(), opts, Some(&status(id.relocated_to(3), true, false))).is_none());
    }

    #[test]
    fn candidates_exclude_other_groups_ordinary_offline_and_unready_owners() {
        let group = SharedGroup::from_static("workers");
        let route_id = Id::from(2, "consumer".into());
        let publisher = From::from_custom(Id::from(1, "publisher".into()));
        let opts = options(Some("workers"), QoS::ExactlyOnce, 7, false);
        let ready = status(route_id.clone(), true, false);
        let allowed = |id: &Id, opts: &SubscriptionOptions, owner: Option<&ClientStatus>| {
            candidate_allowed(&group, id, opts, owner, &publisher, "retired", &HashSet::new())
        };
        assert!(allowed(&route_id, &opts, Some(&ready)));
        assert!(!allowed(&route_id, &options(None, QoS::ExactlyOnce, 7, false), Some(&ready)));
        assert!(!allowed(
            &route_id,
            &options(Some("other-workers"), QoS::ExactlyOnce, 7, false),
            Some(&ready)
        ));
        assert!(!allowed(&route_id, &opts, Some(&status(route_id.clone(), false, false))));
        assert!(!allowed(&route_id, &opts, Some(&status(route_id.clone(), true, true))));
        assert!(!allowed(&route_id, &opts, None));
        assert!(!allowed(&route_id, &opts, Some(&status(route_id.relocated_to(3), true, false))));
        assert!(!candidate_allowed(
            &group,
            &route_id,
            &opts,
            Some(&ready),
            &publisher,
            "consumer",
            &HashSet::new()
        ));
        assert!(!candidate_allowed(
            &group,
            &route_id,
            &opts,
            Some(&ready),
            &publisher,
            "retired",
            &HashSet::from([2])
        ));
        assert!(!candidate_allowed(
            &group,
            &route_id,
            &options(Some("workers"), QoS::ExactlyOnce, 7, true),
            Some(&ready),
            &From::from_custom(route_id.clone()),
            "retired",
            &HashSet::new()
        ));
    }

    #[test]
    fn diagnostics_count_overlapping_rejections_and_bound_samples() {
        let publisher = From::from_custom(Id::from(1, "publisher".into()));
        let group = SharedGroup::from_static("workers");
        let opts = options(Some("workers"), QoS::ExactlyOnce, 7, false);
        let mut diagnostics =
            SharedCandidateDiagnostics { group_state: "valid", group: Some(group), ..Default::default() };
        for index in 0..8 {
            let route_id = Id::from(2, format!("consumer-{index}").into());
            let owner = status(route_id.relocated_to(3), false, true);
            diagnostics.observe_candidate(
                &route_id,
                &opts,
                Some(&owner),
                &publisher,
                "consumer-0",
                &HashSet::from([2]),
            );
        }
        assert_eq!(diagnostics.filter_routes, 8);
        assert_eq!(diagnostics.same_group, 8);
        assert_eq!(diagnostics.eligible, 0);
        assert_eq!(diagnostics.old_client, 1);
        assert_eq!(diagnostics.excluded_node, 8);
        assert_eq!(diagnostics.offline, 8);
        assert_eq!(diagnostics.handshaking, 8);
        assert_eq!(diagnostics.owner_mismatch, 8);
        assert_eq!(diagnostics.missing_status, 0);
        assert_eq!(diagnostics.samples.len(), 6);
        let sample = &diagnostics.samples[0];
        assert_eq!(sample.client_id, "consumer-0");
        assert_eq!(sample.node_id, 2);
        assert!(sample.same_group && sample.old_client && sample.excluded_node);
        assert_eq!(sample.online, Some(false));
        assert_eq!(sample.handshaking, Some(true));
        assert_eq!(sample.owner_matches, Some(false));
        assert!(!sample.no_local && !sample.eligible);

        let route_id = publisher.id.clone();
        diagnostics.observe_candidate(
            &route_id,
            &options(Some("workers"), QoS::ExactlyOnce, 7, true),
            None,
            &publisher,
            "retired",
            &HashSet::new(),
        );
        assert_eq!(diagnostics.missing_status, 1);
        assert_eq!(diagnostics.no_local, 1);
        assert_eq!(diagnostics.samples.len(), 6);
    }

    #[tokio::test]
    async fn replacement_isolates_filter_and_uses_new_members_options() {
        use super::super::message::Message;
        use rmqtt_raft::Store;

        let scx = ServerContext::new()
            .node_id(1)
            .busy_check_enable(false)
            .plugins_config_map_add(
                "rmqtt-cluster-raft",
                "node_grpc_addrs = []\nraft_peer_addrs = []\ntask_exec_queue_workers = 1",
            )
            .build()
            .await;
        let plugin = crate::ClusterPlugin::new(scx.clone(), "rmqtt-cluster-raft").await.unwrap();
        let mut router = plugin.router.clone();
        let old_opts = options(Some("workers"), QoS::ExactlyOnce, 1, false);
        let relation = (
            "sensor/#".into(),
            "retired".into(),
            old_opts,
            None,
            Some(("workers".into(), true, vec!["retired".into()])),
        );
        for (filter, client, group, qos, sub_id) in [
            ("sensor/#", "retired", Some("workers"), QoS::ExactlyOnce, 1),
            ("sensor/#", "replacement", Some("workers"), QoS::AtLeastOnce, 9),
            ("sensor/#", "other-group", Some("other"), QoS::ExactlyOnce, 2),
            ("sensor/#", "ordinary", None, QoS::ExactlyOnce, 3),
            ("sensor/+/temperature", "other-filter", Some("workers"), QoS::ExactlyOnce, 4),
        ] {
            let id = Id::from(2, client.into());
            router._inner().add(filter, id.clone(), options(group, qos, sub_id, false)).await.unwrap();
            router.apply(&Message::Connected { id }.encode().unwrap()).await.unwrap();
        }
        let publisher = From::from_custom(Id::from(1, "publisher".into()));
        let original_id = router.id("retired").unwrap();
        let bound =
            bind_current_shared_relation(&router, &relation.0, &original_id, &"workers".into(), &publisher)
                .unwrap();
        assert_eq!(bound.2, relation.2);
        assert_eq!(bound.3.as_ref().unwrap()[0].get(), 1);
        assert!(is_shared_relation_current(&router, &bound));
        assert!(bind_current_shared_relation(
            &router,
            &"sensor/+/temperature".into(),
            &original_id,
            &"workers".into(),
            &publisher,
        )
        .is_none());
        assert!(bind_current_shared_relation(
            &router,
            &relation.0,
            &original_id,
            &"other".into(),
            &publisher,
        )
        .is_none());
        assert!(bind_current_shared_relation(
            &router,
            &relation.0,
            &original_id.relocated_to(3),
            &"workers".into(),
            &publisher,
        )
        .is_none());
        let diagnostics =
            shared_candidate_diagnostics(&router, &relation, &publisher, "retired", &HashSet::new());
        assert_eq!(diagnostics.group_state, "valid");
        assert_eq!(diagnostics.group.as_deref(), Some("workers"));
        assert_eq!(diagnostics.filter_routes, 4);
        assert_eq!(diagnostics.same_group, 2);
        assert_eq!(diagnostics.eligible, 1);
        assert_eq!(diagnostics.old_client, 1);
        let excluded_diagnostics =
            shared_candidate_diagnostics(&router, &relation, &publisher, "retired", &HashSet::from([2]));
        assert_eq!(excluded_diagnostics.excluded_node, 4);
        assert_eq!(excluded_diagnostics.eligible, 0);

        let mut conflicting_relation = relation.clone();
        conflicting_relation.2 = options(Some("other"), QoS::ExactlyOnce, 1, false);
        let conflicting = shared_candidate_diagnostics(
            &router,
            &conflicting_relation,
            &publisher,
            "retired",
            &HashSet::new(),
        );
        assert_eq!(conflicting.group_state, "conflicting");
        assert!(conflicting.group.is_none());
        assert_eq!(conflicting.filter_routes, 4);
        assert_eq!(conflicting.eligible, 0);
        let mut ordinary_relation = relation.clone();
        ordinary_relation.2 = options(None, QoS::ExactlyOnce, 1, false);
        ordinary_relation.4 = None;
        let ordinary =
            shared_candidate_diagnostics(&router, &ordinary_relation, &publisher, "retired", &HashSet::new());
        assert_eq!(ordinary.group_state, "missing");
        assert_eq!(ordinary.eligible, 0);
        let mut selection = SharedSelectionDiagnostic::default();
        assert!(select_shared_replacement(
            &scx,
            &router,
            &conflicting_relation,
            &publisher,
            "retired",
            &HashSet::new(),
            &mut selection,
        )
        .await
        .is_none());
        assert_eq!(selection.outcome, "invalid_group");
        assert_eq!(selection.candidates, 0);
        let (node_id, selected) = select_shared_replacement(
            &scx,
            &router,
            &relation,
            &publisher,
            "retired",
            &HashSet::new(),
            &mut selection,
        )
        .await
        .unwrap();
        assert_eq!(selection.outcome, "selected");
        assert_eq!(selection.candidates, 1);
        assert_eq!(selection.selected_index, Some(0));
        assert_eq!(selection.selected_client.as_deref(), Some("replacement"));
        assert_eq!(selection.selected_node, Some(2));
        assert_eq!(node_id, 2);
        assert_eq!(selected.0, "sensor/#");
        assert_eq!(selected.1, "replacement");
        assert_eq!(selected.2.qos(), QoS::AtLeastOnce);
        assert_eq!(selected.3.unwrap()[0].get(), 9);
        assert_eq!(selected.4.as_ref().unwrap().0, "workers");
        assert_eq!(selected.4.as_ref().unwrap().2, vec![rmqtt::types::ClientId::from_static("replacement")]);

        let replacement_id = router.id("replacement").unwrap();
        let selected = (selected.0, selected.1, selected.2, None, selected.4);
        assert!(is_shared_relation_current(&router, &selected));
        assert!(select_shared_replacement(
            &scx,
            &router,
            &relation,
            &publisher,
            "retired",
            &HashSet::from([2]),
            &mut selection,
        )
        .await
        .is_none());
        assert_eq!(selection.outcome, "chooser_none");
        assert_eq!(selection.candidates, 0);
        assert!(selection.selected_index.is_none());
        assert!(selection.selected_client.is_none());
        assert!(selection.selected_node.is_none());

        struct FixedChooser(Option<(usize, bool)>);
        #[async_trait::async_trait]
        impl rmqtt::subscribe::SharedSubscription for FixedChooser {
            async fn choice(
                &self,
                _scx: &ServerContext,
                _choices: &[(
                    NodeId,
                    ClientId,
                    SubscriptionOptions,
                    Option<Vec<rmqtt::types::SubscriptionIdentifier>>,
                    Option<bool>,
                )],
            ) -> Option<(usize, bool)> {
                self.0
            }
        }
        for (choice, expected) in [
            (None, "chooser_none"),
            (Some((0, false)), "chooser_offline"),
            (Some((10, true)), "chooser_index_out_of_range"),
        ] {
            *scx.extends.shared_subscription_mut().await = Box::new(FixedChooser(choice));
            assert!(select_shared_replacement(
                &scx,
                &router,
                &relation,
                &publisher,
                "retired",
                &HashSet::new(),
                &mut selection,
            )
            .await
            .is_none());
            assert_eq!(selection.outcome, expected);
            assert_eq!(selection.candidates, 1);
            assert_eq!(selection.selected_index, choice.map(|(index, _)| index));
        }

        // Exercise a route mutation from inside an asynchronous chooser. This
        // must neither deadlock on a held route guard nor return the stale member.
        struct RemovingChooser {
            router: ClusterRouter,
            member_id: Id,
        }
        #[async_trait::async_trait]
        impl rmqtt::subscribe::SharedSubscription for RemovingChooser {
            async fn choice(
                &self,
                _scx: &ServerContext,
                _choices: &[(
                    NodeId,
                    rmqtt::types::ClientId,
                    SubscriptionOptions,
                    Option<Vec<rmqtt::types::SubscriptionIdentifier>>,
                    Option<bool>,
                )],
            ) -> Option<(usize, bool)> {
                self.router._inner().remove("sensor/#", self.member_id.clone()).await.unwrap();
                Some((0, true))
            }
        }
        *scx.extends.shared_subscription_mut().await =
            Box::new(RemovingChooser { router: router.clone(), member_id: replacement_id.clone() });
        assert!(tokio::time::timeout(
            std::time::Duration::from_secs(2),
            select_shared_replacement(
                &scx,
                &router,
                &relation,
                &publisher,
                "retired",
                &HashSet::new(),
                &mut selection,
            ),
        )
        .await
        .expect("route guards must be released before awaiting the chooser")
        .is_none());
        assert_eq!(selection.outcome, "selected_route_removed");
        assert_eq!(selection.candidates, 1);
        assert_eq!(selection.selected_client.as_deref(), Some("replacement"));
        router._inner().add("sensor/#", replacement_id.clone(), selected.2.clone()).await.unwrap();
        router.apply(&Message::Disconnected { id: replacement_id }.encode().unwrap()).await.unwrap();
        assert!(!is_shared_relation_current(&router, &selected));
        assert!(select_shared_replacement(
            &scx,
            &router,
            &relation,
            &publisher,
            "retired",
            &HashSet::new(),
            &mut selection,
        )
        .await
        .is_none());
    }
}
