#nullable enable

using System.Linq;
using EventStore.Core.Configuration;
using EventStore.Core.Configuration.Sources;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Configuration;

public class ClusterVNodeOptionsTests {
	static ClusterVNodeOptions GetOptions(string args) {
		// we always need to override the config file for testing
		var configuration = EventStoreConfiguration.Build(args.Split().Concat(["--config", "testing"]).ToArray());
		return ClusterVNodeOptions.FromConfiguration(configuration);
	}

	[Fact]
	public void confirm_suggested_option() {
		var options = GetOptions("--cluster-sze 3");

		var (key, value) = options.Unknown.Options[0];
		Assert.Equal("ClusterSze", key);
		Assert.Equal("ClusterSize", value);
	}

	[Fact]
	public void when_we_dont_have_that_option() {
		var options = GetOptions("--something-that-is-wildly-off 3");

		var (key, value) = options.Unknown.Options[0];
		Assert.Equal("SomethingThatIsWildlyOff", key);
		Assert.Equal("", value);
	}

	[Fact]
	public void valid_parameters() {
		var options = GetOptions("--cluster-size 3");

		var values = options.Unknown.Options;
		Assert.Empty(values);
	}

	[Fact]
	public void four_characters_off() {
		var options = GetOptions("--cluse-ie 3");

		var (key, value) = options.Unknown.Options[0];
		Assert.Equal("CluseIe", key);
		Assert.Equal("ClusterSize", value);
	}
	
	[Fact]
	public void print_help_text() {
		var helpText = ClusterVNodeOptions.HelpText;
		helpText.Should().NotBeEmpty();
	}

	[Fact]
	public void ignores_subsection_arguments() {
		var options = GetOptions(
			"--EventStore:Metrics:A aaa " +
			"--EventStore:Plugins:B bbb"
		);

		Assert.Fail("Fix this sergio!");
		Assert.Null(options.ConfigurationRoot["EventStore"]);
		Assert.Empty(options.Unknown.Options);
	}

	[Fact]
	public void BindWorks() {
		var config = new ConfigurationBuilder()
			.AddEventStoreDefaultValues()
			.Build();
			
		var manual = ClusterVNodeOptions.FromConfiguration(config);
		var binded = ClusterVNodeOptions.BindFromConfiguration(config);
			
		manual.Should().BeEquivalentTo(binded);
	}

	// [Fact]
	// public void BindCommaSeparatedValuesOption() {
	// 	EndPoint[] endpoints = [new IPEndPoint(IPAddress.Loopback, 1113), new DnsEndPoint("some-host", 1114)];
	// 		
	// 	var values = string.Join(",", endpoints.Select(x => $"{x}"));
	//
	// 	var config = new ConfigurationBuilder()
	// 		.AddInMemoryCollection(new KeyValuePair<string, string>[] {
	// 			new("GossipSeed", values),
	// 			new("FakeEndpoint", endpoints[0].ToString())
	// 		})
	// 		.Build();
	// 		
	// 	var options = config.Get<ClusterVNodeOptions.ClusterOptions>();
	//
	// 	options.GossipSeed.Should().BeEquivalentTo(endpoints);
	// }
}
