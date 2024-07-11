using DotNext.Threading;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Timeout = System.Threading.Timeout;

namespace EventStore.Connectors.Control;

public abstract class LeadershipAwareService : BackgroundService {
    protected LeadershipAwareService(INodeLifetimeService nodeLifetime, ILoggerFactory loggerFactory) {
        NodeLifetime = nodeLifetime;
        Logger       = loggerFactory.CreateLogger(GetType().Name);
    }

    INodeLifetimeService NodeLifetime { get; }
    ILogger              Logger       { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        Logger.LogServiceStarted();

        while (!stoppingToken.IsCancellationRequested) {
            var leadershipToken = await NodeLifetime.WaitForLeadershipAsync(Timeout.InfiniteTimeSpan, stoppingToken);
            var token           = leadershipToken;

            var cancellator = token.LinkTo(stoppingToken);

            try {
                // it only runs on a leader node, so if the cancellation
                // token is cancelled, it means the node lost leadership
                await Execute(cancellator!.Token);
            }
            catch (OperationCanceledException) when (cancellator?.CancellationOrigin == stoppingToken) {
                Logger.LogServiceStopped();
                break;
            }
            finally {
                cancellator?.Dispose();
            }
        }
    }

    protected abstract Task Execute(CancellationToken stoppingToken);
}

static partial class LeadershipAwareServiceLogMessages {
    [LoggerMessage(LogLevel.Debug, "Service started")]
    internal static partial void LogServiceStarted(this ILogger logger);

    [LoggerMessage(LogLevel.Debug, "Service stopped")]
    internal static partial void LogServiceStopped(this ILogger logger);
}