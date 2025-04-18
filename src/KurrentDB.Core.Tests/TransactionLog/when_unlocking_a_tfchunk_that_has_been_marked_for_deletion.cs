// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using KurrentDB.Core.TransactionLog.Chunks.TFChunk;
using NUnit.Framework;

namespace KurrentDB.Core.Tests.TransactionLog;

[TestFixture]
public class when_unlocking_a_tfchunk_that_has_been_marked_for_deletion : SpecificationWithFile {
	private TFChunk _chunk;

	[SetUp]
	public override async Task SetUp() {
		await base.SetUp();
		_chunk = await TFChunkHelper.CreateNewChunk(Filename, 1000);
		await _chunk.Complete(CancellationToken.None);
		var reader = await _chunk.AcquireRawReader(CancellationToken.None);
		_chunk.MarkForDeletion();
		reader.Release();
	}

	[Test]
	public void the_file_is_deleted() {
		Assert.IsFalse(File.Exists(Filename));
	}
}
