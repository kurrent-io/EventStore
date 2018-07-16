﻿using System;
using System.Collections.Generic;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Services.Plugins;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using EventStore.Transport.Http.EntityManagement;

namespace EventStore.Core.Services.Transport.Http.Controllers
{
    public class GeoReplicaController : CommunicationController
    {
        private readonly IPublisher _networkSendQueue;
        private readonly IPluginPublisher _pluginPublisher;
        private static readonly ILogger Log = LogManager.GetLoggerFor<GeoReplicaController>();

        private static readonly ICodec[] SupportedCodecs = new ICodec[] { Codec.Text, Codec.Json, Codec.Xml, Codec.ApplicationXml };

        public GeoReplicaController(IPublisher publisher, IPublisher networkSendQueue, IPluginPublisher pluginPublisher) : base(publisher)
        {
            _networkSendQueue = networkSendQueue;
            _pluginPublisher = pluginPublisher;
        }

        protected override void SubscribeCore(IHttpService service)
        {
            service.RegisterAction(new ControllerAction("/georeplica/{servicetype}/{name}/start", HttpMethod.Post, Codec.NoCodecs, SupportedCodecs), OnPostGeoReplicaStart);
            service.RegisterAction(new ControllerAction("/georeplica/{servicetype}/{name}/stop", HttpMethod.Post, Codec.NoCodecs, SupportedCodecs), OnPostGeoReplicaStop);
            service.RegisterAction(new ControllerAction("/georeplica", HttpMethod.Get, Codec.NoCodecs, SupportedCodecs), OnGetStats);
        }

        private void OnGetStats(HttpEntityManager entity, UriTemplateMatch match)
        {
            var sendToHttpEnvelope = new SendToHttpEnvelope(
                _networkSendQueue, entity, Format.GetPluginStatsCompleted,
                (e, m) => Configure.Ok(e.ResponseCodec.ContentType, Helper.UTF8NoBom, null, null, false));
            Publish(new PluginMessage.GetStats(sendToHttpEnvelope));
        }

        private void OnPostGeoReplicaStart(HttpEntityManager entity, UriTemplateMatch match)
        {
            if (entity.User != null && (entity.User.IsInRole(SystemRoles.Admins) || entity.User.IsInRole(SystemRoles.Operations)))
            {
                var serviceType = match.BoundVariables["servicetype"];
                if (serviceType.Equals("dispatcher") || serviceType.Equals("receiver"))
                {
                    var name = match.BoundVariables["name"];
                    Log.Info("Request start GeoReplica because Start command has been received.");
                    ProcessRequest(entity, new Dictionary<string, dynamic>
                    {
                        {"Name", name},
                        {"ServiceType", serviceType},
                        {"Action", "Start"}
                    });
                }
                else
                {
                    entity.ReplyStatus(HttpStatusCode.BadRequest, "servicetype must be 'dispatcher' or 'receiver'", LogReplyError);
                }
            }
            else
            {
                entity.ReplyStatus(HttpStatusCode.Unauthorized, "Unauthorized", LogReplyError);
            }
        }

        private void OnPostGeoReplicaStop(HttpEntityManager entity, UriTemplateMatch match)
        {
            if (entity.User != null && (entity.User.IsInRole(SystemRoles.Admins) || entity.User.IsInRole(SystemRoles.Operations)))
            {
                var serviceType = match.BoundVariables["servicetype"];
                if (serviceType.Equals("dispatcher") || serviceType.Equals("receiver"))
                {
                    var name = match.BoundVariables["name"];
                    Log.Info("Request stop GeoReplica because Stop request has been received.");
                    ProcessRequest(entity,
                        new Dictionary<string, dynamic>
                        {
                            {"Name", name},
                            {"ServiceType", serviceType},
                            {"Action", "Stop"}
                        });
                }
                else
                {
                    entity.ReplyStatus(HttpStatusCode.BadRequest, "servicetype must be 'dispatcher' or 'receiver'", LogReplyError);
                }
            }
            else
            {
                entity.ReplyStatus(HttpStatusCode.Unauthorized, "Unauthorized", LogReplyError);
            }
        }

        private void ProcessRequest(HttpEntityManager entity, IDictionary<string, dynamic> request)
        {
            if (_pluginPublisher.TryPublish(request))
            {
                entity.ReplyStatus(HttpStatusCode.OK, "OK", LogReplyError);
            }
            else
            {
                entity.ReplyStatus(HttpStatusCode.BadRequest, "KO", LogReplyError);
            }
        }

        private void LogReplyError(Exception exc)
        {
            Log.Debug("Error while closing HTTP connection (georeplica controller): {0}.", exc.Message);
        }
    }
}