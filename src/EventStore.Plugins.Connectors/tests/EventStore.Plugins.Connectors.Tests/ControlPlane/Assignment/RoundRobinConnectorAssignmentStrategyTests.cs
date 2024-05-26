using EventStore.Connectors.ControlPlane;
using EventStore.Connectors.ControlPlane.Assignment;
using EventStore.Testing.Fixtures;
using MassTransit;

namespace EventStore.Plugins.Connectors.Tests.ControlPlane.Assignment;

[Trait("Category", "Assignment")]
public class RoundRobinConnectorAssignmentStrategyTests(ITestOutputHelper output, FastFixture fixture) : FastTests(output, fixture) {
	[Fact]
	public void assigns_directly() {
        var topology = ClusterTopology.From(
            new(NewId.Next().ToGuid(), ClusterNodeState.Leader),
            new(NewId.Next().ToGuid(), ClusterNodeState.Follower),
            new(NewId.Next().ToGuid(), ClusterNodeState.ReadOnlyReplica)
        );
		
        ConnectorResource[] connectors = [
            new(NewId.Next().ToGuid(), ClusterNodeState.Leader),
            new(NewId.Next().ToGuid(), ClusterNodeState.Follower),
            new(NewId.Next().ToGuid(), ClusterNodeState.ReadOnlyReplica),
            new(NewId.Next().ToGuid(), ClusterNodeState.Unmapped)
        ];
		
        var expectedResult = new ClusterConnectorsAssignment(Guid.NewGuid(), new() {
            { topology[0], NodeConnectorsAssignment.From([connectors[0]]) },
            { topology[1], NodeConnectorsAssignment.From([connectors[1]]) },
            { topology[2], NodeConnectorsAssignment.From([connectors[2], connectors[3]]) }
        });
        
        var result = new LeastLoadedWithAffinityConnectorAssignor().Assign(topology, connectors);
		
        result.Should().BeEquivalentTo(expectedResult);
	}
}