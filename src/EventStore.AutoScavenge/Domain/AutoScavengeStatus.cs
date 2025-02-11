// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

namespace EventStore.AutoScavenge.Domain;

// Names are serialized on the status endpoint
public enum AutoScavengeStatus {
	// No schedule, do nothing
	NotConfigured,

	// Evaluating whether there are more nodes to scavenge this cycle.
	ContinuingClusterScavenge,

	// A node scavenge is under way and we are waiting for it.
	MonitoringNodeScavenge,

	// A node scavenge is under way and we are trying to stop it.
	Pausing,

	// Paused. No cluster scavenge is under way.
	PausedWhileIdle,

	// Paused. A cluster scavenge is ready to resume.
	PausedWhileScavengingCluster,

	// A node is designated, we are starting a nodescavenge on it.
	StartingNodeScavenge,

	// Waiting for the schedule to begin the next cycle.
	Waiting,
}
