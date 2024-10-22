using DotNext.Threading;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Timeout = System.Threading.Timeout;

namespace EventStore.Connectors.System;

public abstract class LeadershipAwareService : BackgroundService {
    protected LeadershipAwareService(Func<string, INodeLifetimeService> nodeLifetimeBuilder, GetNodeSystemInfo getNodeSystemInfo, ILoggerFactory loggerFactory) {
        NodeLifetime      = nodeLifetimeBuilder(GetType().Name);
        GetNodeSystemInfo = getNodeSystemInfo;
        Logger            = loggerFactory.CreateLogger(GetType().Name);
    }

    INodeLifetimeService NodeLifetime      { get; }
    GetNodeSystemInfo    GetNodeSystemInfo { get; }
    LinkedCancellationTokenSource? Cancellator { get; set; }

    protected ILogger Logger { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        Logger.LogServiceStarted();

        while (!stoppingToken.IsCancellationRequested) {
            try {
                var leadershipToken =
                    await NodeLifetime.WaitForLeadershipAsync(Timeout.InfiniteTimeSpan, stoppingToken);

                var token = leadershipToken;

                Cancellator = token.LinkTo(stoppingToken);

                var nodeInfo = await GetNodeSystemInfo(stoppingToken);

                // it only runs on a leader node, so if the cancellation
                // token is cancelled, it means the node lost leadership
                await Execute(nodeInfo, Cancellator!.Token);
            } catch (OperationCanceledException) when (Cancellator?.CancellationOrigin == stoppingToken) {
                Logger.LogServiceStopped();
                break;
            } catch (ObjectDisposedException) {
                // Node lifetime service reported that server is shutting down.
                break;
            } finally {
                Cancellator?.Dispose();
            }
        }

        NodeLifetime.ReportShutdownCompleted();
    }

    protected abstract Task Execute(NodeSystemInfo nodeInfo, CancellationToken stoppingToken);
}

static partial class LeadershipAwareServiceLogMessages {
    [LoggerMessage(LogLevel.Debug, "Service started")]
    internal static partial void LogServiceStarted(this ILogger logger);

    [LoggerMessage(LogLevel.Debug, "Service stopped")]
    internal static partial void LogServiceStopped(this ILogger logger);
}