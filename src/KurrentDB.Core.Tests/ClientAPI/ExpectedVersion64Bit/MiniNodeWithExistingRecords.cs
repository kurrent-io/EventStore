// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using KurrentDB.Common.Utils;
using KurrentDB.Core.Bus;
using KurrentDB.Core.Data;
using KurrentDB.Core.Helpers;
using KurrentDB.Core.Index;
using KurrentDB.Core.LogAbstraction;
using KurrentDB.Core.Services.Storage.ReaderIndex;
using KurrentDB.Core.Tests.ClientAPI.Helpers;
using KurrentDB.Core.Tests.Helpers;
using KurrentDB.Core.Tests.TransactionLog;
using KurrentDB.Core.TransactionLog.Checkpoint;
using KurrentDB.Core.TransactionLog.Chunks;
using KurrentDB.Core.TransactionLog.LogRecords;
using KurrentDB.Core.Util;
using NUnit.Framework;

namespace KurrentDB.Core.Tests.ClientAPI.ExpectedVersion64Bit;

public abstract class MiniNodeWithExistingRecords<TLogFormat, TStreamId> : SpecificationWithDirectoryPerTestFixture {
	private readonly TcpType _tcpType = TcpType.Ssl;
	protected MiniNode<TLogFormat, TStreamId> Node;

	protected readonly int MaxEntriesInMemTable = 20;
	protected readonly long MetastreamMaxCount = 1;
	protected readonly bool PerformAdditionalCommitChecks = true;
	protected readonly byte IndexBitnessVersion = Opts.IndexBitnessVersionDefault;
	protected LogFormatAbstractor<TStreamId> _logFormatFactory;
	protected TableIndex<TStreamId> TableIndex;
	protected IReadIndex<TStreamId> ReadIndex;

	protected TFChunkDb Db;
	protected TFChunkWriter Writer;
	protected ICheckpoint WriterCheckpoint;
	protected ICheckpoint ChaserCheckpoint;
	protected IODispatcher IODispatcher;
	protected SynchronousScheduler Bus;

	protected IEventStoreConnection _store;

	protected virtual IEventStoreConnection BuildConnection(MiniNode<TLogFormat, TStreamId> node) {
		return TestConnection.To(node, _tcpType);
	}

	[OneTimeSetUp]
	public override async Task TestFixtureSetUp() {
		await base.TestFixtureSetUp();
		string dbPath = Path.Combine(PathName, string.Format("mini-node-db-{0}", Guid.NewGuid()));

		_logFormatFactory = LogFormatHelper<TLogFormat, TStreamId>.LogFormatFactory.Create(new() {
			IndexDirectory = GetFilePathFor("index"),
		});

		Bus = new();
		IODispatcher = new IODispatcher(Bus, Bus);

		if (!Directory.Exists(dbPath))
			Directory.CreateDirectory(dbPath);

		var writerCheckFilename = Path.Combine(dbPath, Checkpoint.Writer + ".chk");
		var chaserCheckFilename = Path.Combine(dbPath, Checkpoint.Chaser + ".chk");

		WriterCheckpoint = new MemoryMappedFileCheckpoint(writerCheckFilename, Checkpoint.Writer);
		ChaserCheckpoint = new MemoryMappedFileCheckpoint(chaserCheckFilename, Checkpoint.Chaser);

		Db = new TFChunkDb(TFChunkHelper.CreateDbConfig(dbPath, WriterCheckpoint, ChaserCheckpoint, TFConsts.ChunkSize));
		await Db.Open();

		// create DB
		Writer = new TFChunkWriter(Db);
		await Writer.Open(CancellationToken.None);

		var pm = _logFormatFactory.CreatePartitionManager(
			reader: new TFChunkReader(Db, WriterCheckpoint),
			writer: Writer);
		await pm.Initialize(CancellationToken.None);

		await WriteTestScenario(CancellationToken.None);

		await Writer.DisposeAsync();
		Writer = null;
		WriterCheckpoint.Flush();
		ChaserCheckpoint.Write(WriterCheckpoint.Read());
		ChaserCheckpoint.Flush();
		await Db.DisposeAsync();

		// start node with our created DB
		Node = new MiniNode<TLogFormat, TStreamId>(PathName, inMemDb: false, dbPath: dbPath);
		await Node.Start();

		try {
			await Given().WithTimeout();
		} catch (Exception ex) {
			throw new Exception("Given Failed", ex);
		}
	}

	[OneTimeTearDown]
	public override async Task TestFixtureTearDown() {
		_store?.Dispose();
		_logFormatFactory?.Dispose();

		await Node.Shutdown();
		await base.TestFixtureTearDown();
	}

	public abstract ValueTask WriteTestScenario(CancellationToken token);
	public abstract Task Given();

	protected async ValueTask<EventRecord> WriteSingleEvent(string eventStreamName,
		long eventNumber,
		string data,
		DateTime? timestamp = null,
		Guid eventId = default(Guid),
		string eventType = "some-type",
		CancellationToken token = default) {

		long pos = Writer.Position;
		_logFormatFactory.StreamNameIndex.GetOrReserve(
			_logFormatFactory.RecordFactory,
			eventStreamName,
			pos,
			out var eventStreamId,
			out var streamRecord);

		if (streamRecord is not null) {
			(_, pos) = await Writer.Write(streamRecord, token);
		}

		_logFormatFactory.EventTypeIndex.GetOrReserveEventType(
			_logFormatFactory.RecordFactory,
			eventType,
			pos,
			out var eventTypeId,
			out var eventTypeRecord);

		if (eventTypeRecord != null) {
			(_, pos) = await Writer.Write(eventTypeRecord, token);
		}

		var prepare = LogRecord.SingleWrite(
			_logFormatFactory.RecordFactory,
			pos,
			eventId == default(Guid) ? Guid.NewGuid() : eventId,
			Guid.NewGuid(),
			eventStreamId,
			eventNumber - 1,
			eventTypeId,
			Helper.UTF8NoBom.GetBytes(data),
			null,
			timestamp);

		(var written, pos) = await Writer.Write(prepare, token);
		Assert.IsTrue(written);
		var commit = LogRecord.Commit(pos, prepare.CorrelationId, prepare.LogPosition,
			eventNumber);
		Assert.IsTrue(await Writer.Write(commit, token) is (true, _));
		Assert.AreEqual(eventStreamId, prepare.EventStreamId);

		var eventRecord = new EventRecord(eventNumber, prepare, eventStreamName, eventType);
		return eventRecord;
	}
}
