// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Threading.Tasks;
using KurrentDB.Core.Services;
using KurrentDB.Core.Synchronization;
using KurrentDB.Core.Tests.Bus;
using KurrentDB.Core.Tests.Services.Storage;
using NUnit.Framework;

namespace KurrentDB.Core.Tests.Services.RedactionService;

[TestFixture]
public abstract class RedactionServiceTestFixture<TLogFormat, TStreamId> : ReadIndexTestScenario<TLogFormat, TStreamId> {
	private SemaphoreSlimLock _switchChunksLock;
	public RedactionService<TStreamId> RedactionService { get; private set; }

	public RedactionServiceTestFixture() : base(chunkSize: 1024) { }

	[SetUp]
	public virtual Task SetUp() {
		_switchChunksLock = new SemaphoreSlimLock();
		RedactionService = new RedactionService<TStreamId>(new FakeQueuedHandler(), Db, ReadIndex, _switchChunksLock);
		return Task.CompletedTask;
	}

	[TearDown]
	public virtual Task TearDown() {
		_switchChunksLock?.Dispose();
		return Task.CompletedTask;
	}
}
