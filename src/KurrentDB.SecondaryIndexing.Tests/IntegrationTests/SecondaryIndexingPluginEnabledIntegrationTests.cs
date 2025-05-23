// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Text;
using KurrentDB.SecondaryIndexing.Indices.Category;
using KurrentDB.SecondaryIndexing.Indices.Default;
using KurrentDB.SecondaryIndexing.Tests.IntegrationTests.Fixtures;
using Xunit.Abstractions;

namespace KurrentDB.SecondaryIndexing.Tests.IntegrationTests;

[Trait("Category", "Integration")]
[Collection("SecondaryIndexingPluginEnabled")]
public class SecondaryIndexingPluginEnabledIntegrationTests_LogV2(
	SecondaryIndexingEnabledFixture fixture,
	ITestOutputHelper output
) : SecondaryIndexingPluginEnabledIntegrationTests<string>(fixture, output);

[Trait("Category", "Integration")]
[Collection("SecondaryIndexingPluginEnabled")]
public class SecondaryIndexingPluginEnabledIntegrationTests_LogV3(
	SecondaryIndexingEnabledFixture fixture,
	ITestOutputHelper output
) : SecondaryIndexingPluginEnabledIntegrationTests<string>(fixture, output);

public abstract class SecondaryIndexingPluginEnabledIntegrationTests<TStreamId>(
	SecondaryIndexingEnabledFixture fixture,
	ITestOutputHelper output
) : SecondaryIndexingPluginIntegrationTest(fixture, output) {
	private readonly string[] _expectedEventData = ["""{"test":"123"}""", """{"test":"321"}"""];

	[Fact]
	public async Task ReadsIndexStream_ForEnabledPlugin() {
		// Given
		var streamName = RandomStreamName();
		var appendResult = await fixture.AppendToStream(streamName, _expectedEventData);

		// When
		var allReadResult = await fixture.ReadUntil(DefaultIndexConstants.IndexName, appendResult.Position);
		var categoryReadResult = await fixture.ReadUntil($"{CategoryIndexConstants.IndexPrefix}test", appendResult.Position);

		// Then
		Assert.NotEmpty(allReadResult);
		Assert.NotEmpty(categoryReadResult);

		var allResults = allReadResult.Where(e => e.Event.EventStreamId == streamName).ToList();
		var categoryResults = allReadResult.Where(e => e.Event.EventStreamId == streamName).ToList();

		Assert.Equal(_expectedEventData.Length, allResults.Count);
		Assert.Equal(_expectedEventData.Length, categoryResults.Count);

		Assert.All(allResults, e => Assert.Contains(e.Event.DebugDataView, _expectedEventData));
		Assert.All(categoryResults, e => Assert.Contains(e.Event.DebugDataView, _expectedEventData));
	}
}
