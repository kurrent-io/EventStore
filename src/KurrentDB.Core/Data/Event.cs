// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using KurrentDB.Common.Utils;
using KurrentDB.Core.TransactionLog.Chunks;
using JetBrains.Annotations;

namespace KurrentDB.Core.Data;

/// <summary>
/// Represents schema information that includes the schema format, and version identifier.
/// </summary>
public record SchemaInfo(SchemaInfo.SchemaDataFormat SchemaFormat, Guid SchemaVersionId) {
    public static readonly SchemaInfo None = new(0, Guid.Empty);

    public byte[] ToByteArray() {
        var result = new byte[18];
        // BitConverter.TryWriteBytes(result.AsSpan(0, 1), 0);
        // BitConverter.TryWriteBytes(result.AsSpan(0, sizeof(ushort)), SchemaFormat);
        // BitConverter.TryWriteBytes(result.AsSpan(sizeof(ushort)), SchemaVersionId.ToByteArray());
        return result;
    }

    public enum SchemaDataFormat {
        Undefined = 0,
        Json      = 1,
        Protobuf  = 2,
        Avro      = 3,
        Bytes     = 4
    }
}

public class Event {
	public readonly Guid EventId;
	public readonly string EventType;
	public readonly bool IsJson;
	public readonly byte[] Data;
	public readonly byte[] Metadata;

    public readonly SchemaInfo MetadataSchemaInfo;
    public readonly SchemaInfo DataSchemaInfo;

	public Event(Guid eventId, string eventType, bool isJson, string data, string metadata, SchemaInfo dataSchemaInfo, SchemaInfo metadataSchemaInfo)
		: this(
			eventId, eventType, isJson, Helper.UTF8NoBom.GetBytes(data),
			metadata != null ? Helper.UTF8NoBom.GetBytes(metadata) : null, dataSchemaInfo, metadataSchemaInfo) {
	}

	public static int SizeOnDisk(string eventType, byte[] data, byte[] metadata) =>
		(data?.Length ?? 0) + (metadata?.Length ?? 0) + (eventType.Length * 2);

	private static bool ExceedsMaximumSizeOnDisk(string eventType, byte[] data, byte[] metadata) =>
		SizeOnDisk(eventType, data, metadata) > TFConsts.EffectiveMaxLogRecordSize;

	public Event(Guid eventId, string eventType, bool isJson, byte[] data, byte[] metadata, SchemaInfo dataSchemaInfo, SchemaInfo metadataSchemaInfo) {
		if (eventId == Guid.Empty)
			throw new ArgumentException("Empty eventId provided.", nameof(eventId));
		if (string.IsNullOrEmpty(eventType))
			throw new ArgumentException("Empty eventType provided.", nameof(eventType));
		if (ExceedsMaximumSizeOnDisk(eventType, data, metadata))
			throw new ArgumentException("Record is too big.", nameof(data));

		EventId = eventId;
		EventType = eventType;
		IsJson = isJson;
		Data = data ?? Array.Empty<byte>();
		Metadata = metadata ?? Array.Empty<byte>();
        DataSchemaInfo = dataSchemaInfo ?? SchemaInfo.None;
        MetadataSchemaInfo = metadataSchemaInfo ?? SchemaInfo.None;
	}

	public Event(Guid eventId, string eventType, bool isJson, byte[] data, byte[] metadata)
		: this(eventId, eventType, isJson, data, metadata, SchemaInfo.None, SchemaInfo.None) {
	}

	public Event(Guid eventId, string eventType, bool isJson, string data, string metadata)
		: this(eventId, eventType, isJson, data, metadata, SchemaInfo.None, SchemaInfo.None) {
	}
}
