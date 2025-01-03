// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

namespace EventStore.Core.TransactionLog.FileNamingStrategy;

// This abstracts a translation from logical chunk numbers (indexes) and
// chunk versions, to chunk files in storage. Strictly it is more than a naming strategy
// because it also reads the storage to list the chunks.
public interface IVersionedFileNamingStrategy {
	// Pure naming strategy methods
	string GetFilenameFor(int index, int version);
	string CreateTempFilename();
	int GetIndexFor(string fileName);
	int GetVersionFor(string fileName);
	string GetPrefixFor(int? index, int? version);

	// Methods that rely on the state of the storage
	string[] GetAllVersionsFor(int index);
	string[] GetAllPresentFiles();
	string[] GetAllTempFiles();

	// When we are creating a new version of a chunk at the given index, this determines
	// the correct file name for it based on the existing version(s).
	// defaultVersion is used if there are no existing versions.
	string DetermineNewVersionFilenameForIndex(int index, int defaultVersion);
}
