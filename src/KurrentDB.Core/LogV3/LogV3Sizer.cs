// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using KurrentDB.Core.LogAbstraction;
using StreamId = System.UInt32;

namespace KurrentDB.Core.LogV3;

public class LogV3Sizer : ISizer<StreamId> {
	public int GetSizeInBytes(StreamId t) => sizeof(StreamId);
}
