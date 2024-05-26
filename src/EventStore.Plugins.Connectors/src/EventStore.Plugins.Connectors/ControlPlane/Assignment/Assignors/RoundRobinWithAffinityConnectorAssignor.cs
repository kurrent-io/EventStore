using EventStore.Streaming;

namespace EventStore.Connectors.ControlPlane.Assignment;

public class RoundRobinWithAffinityConnectorAssignor : AffinityConnectorAssignorBase {
    public override ConnectorAssignmentStrategy Type => ConnectorAssignmentStrategy.RoundRobinWithAffinity;

    protected override IEnumerable<(ConnectorId ConnectorId, ClusterNodeId NodeId)> AssignConnectors(
        ClusterNode[] clusterNodes, 
        ConnectorResource[] connectors,
        ClusterConnectorsAssignment currentClusterAssignment
    ) {
        var assignments = connectors.Select(x => AssignConnector(x.ConnectorId, clusterNodes));

        return assignments;

        static (ConnectorId ConnectorId, ClusterNodeId NodeId) AssignConnector(ConnectorId connectorId, IReadOnlyList<ClusterNode> clusterNodes) {
            var nodeIndex = (int)(HashGenerators.MurmurHash3(connectorId) % clusterNodes.Count);
            var nodeId    = clusterNodes[nodeIndex].NodeId;
            return new(connectorId, nodeId);
        }
    }
}