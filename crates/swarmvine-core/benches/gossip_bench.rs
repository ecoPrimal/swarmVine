// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 ecoPrimals Project
#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use swarmvine_core::{GossipEngine, GossipEntry, GossipTopic};

fn bench_inject(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let engine = GossipEngine::new("bench-gate".to_owned());

    c.bench_function("gossip_inject", |b| {
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            rt.block_on(engine.inject(
                GossipTopic::Tower,
                format!("capability.advertise:gate{seq}"),
                serde_json::json!({"methods": ["health.check"]}),
            ));
        });
    });
}

fn bench_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let engine = GossipEngine::new("bench-gate".to_owned());

    // Pre-populate
    rt.block_on(async {
        for i in 0..100 {
            engine
                .inject(
                    GossipTopic::Data,
                    format!("cas.have:gate{i}"),
                    serde_json::json!({"hashes": ["abc123"]}),
                )
                .await;
        }
    });

    c.bench_function("gossip_query_full_topic", |b| {
        b.iter(|| {
            rt.block_on(engine.query(black_box(GossipTopic::Data), None));
        });
    });

    c.bench_function("gossip_query_with_prefix", |b| {
        b.iter(|| {
            rt.block_on(engine.query(GossipTopic::Data, Some(black_box("cas.have:gate5"))));
        });
    });
}

fn bench_nonce_dedup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let engine = GossipEngine::new("bench-gate".to_owned());

    // Inject one entry to get its nonce
    let entry = GossipEntry::new(
        GossipTopic::Tower,
        "endpoint.alive:benchGate",
        serde_json::json!({}),
        "bench-gate",
    );
    let nonce = entry.nonce.clone();
    rt.block_on(engine.ingest(entry, None));

    // Create duplicate with same nonce
    c.bench_function("gossip_nonce_dedup_reject", |b| {
        b.iter(|| {
            let dup = GossipEntry {
                topic: GossipTopic::Tower,
                key: "endpoint.alive:benchGate".into(),
                payload: serde_json::json!({}),
                origin_gate: "other-gate".into(),
                nonce: black_box(nonce.clone()),
                ttl: 8,
                version: 1,
                created_at_epoch: 0,
                expires_at_epoch: u64::MAX,
            };
            rt.block_on(engine.ingest(dup, Some("peer-a")));
        });
    });
}

criterion_group!(benches, bench_inject, bench_query, bench_nonce_dedup);
criterion_main!(benches);
