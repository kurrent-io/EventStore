// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using EventStore.Core.Services.Archive.Naming;
using FluentStorage;
using FluentStorage.Blobs;
using Serilog;

namespace EventStore.Core.Services.Archive.Storage;

public class S3Writer : FluentWriter, IArchiveStorageWriter {
	public S3Writer(S3Options options, IArchiveChunkNameResolver chunkNameResolver, string archiveCheckpointFile)
		: base(chunkNameResolver, archiveCheckpointFile) {
		BlobStorage = StorageFactory.Blobs.AwsS3(
			awsCliProfileName: options.AwsCliProfileName,
			bucketName: options.Bucket,
			region: options.Region);
	}

	protected override ILogger Log { get; } = Serilog.Log.ForContext<S3Writer>();

	protected override FluentStorage.Blobs.IBlobStorage BlobStorage { get; }
}
