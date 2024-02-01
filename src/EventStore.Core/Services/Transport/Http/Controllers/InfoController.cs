using System;
using System.Linq;
using System.Collections.Generic;
using EventStore.Common.Utils;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using EventStore.Transport.Http.EntityManagement;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Plugins.Authentication;
using EventStore.Plugins.Authorization;
using ILogger = Serilog.ILogger;

namespace EventStore.Core.Services.Transport.Http.Controllers;

public class InfoController : IHttpController, IHandle<SystemMessage.StateChangeMessage> {
	static readonly ILogger Log = Serilog.Log.ForContext<InfoController>();
	static readonly ICodec[] SupportedCodecs = {Codec.Json, Codec.Xml, Codec.ApplicationXml, Codec.Text};

	readonly ClusterVNodeOptions _options;
	readonly IDictionary<string, bool> _features;
	readonly IAuthenticationProvider _authenticationProvider;
	VNodeState _currentState;

	public InfoController(ClusterVNodeOptions options, IDictionary<string, bool> features, IAuthenticationProvider authenticationProvider) {
		_options                = options;
		_features               = features;
		_authenticationProvider = authenticationProvider;
	}

	public void Subscribe(IHttpService service) {
		Ensure.NotNull(service, "service");

		service.RegisterAction(
			new("/info", HttpMethod.Get, Codec.NoCodecs, SupportedCodecs, new Operation(Operations.Node.Information.Read)), 
			OnGetInfo
		);
		service.RegisterAction(
			new("/info/options", HttpMethod.Get, Codec.NoCodecs, SupportedCodecs, new Operation(Operations.Node.Information.Options)), 
			OnGetOptions
		);
	}
	
	public void Handle(SystemMessage.StateChangeMessage message) => 
		_currentState = message.State;

	void OnGetInfo(HttpEntityManager entity, UriTemplateMatch match) {
		entity.ReplyTextContent(
			Codec.Json.To(
				new {
					ESVersion      = VersionInfo.Version,
					State          = _currentState.ToString().ToLower(),
					Features       = _features,
					Authentication = GetAuthenticationInfo()
				}
			),
			HttpStatusCode.OK,
			"OK",
			entity.ResponseCodec.ContentType,
			null,
			e => Log.Error(e, "Error while writing HTTP response (info)")
		);
	}

	Dictionary<string, object> GetAuthenticationInfo() {
		if (_authenticationProvider == null)
			return null;

		return new() {
			{ "type", _authenticationProvider.Name },
			{ "properties", _authenticationProvider.GetPublicProperties() }
		};
	}

	void OnGetOptions(HttpEntityManager entity, UriTemplateMatch match) {
		if (entity.User != null && (entity.User.LegacyRoleCheck(SystemRoles.Operations) || entity.User.LegacyRoleCheck(SystemRoles.Admins))) {
			var options = _options.DisplayOptions.Values.Select(
				x => new OptionStructure {
					Name           = x.Title,
					Description    = x.Metadata.Description,
					Group          = x.Metadata.SectionName,
					PossibleValues = x.Metadata.AllowedValues,
					Value          = x.Value
				}
			);
				
			// var options = _options.GetPrintableOptions()?.Select(
			// 	x => new OptionStructure {
			// 		Name           = x.Name,
			// 		Description    = x.Description,
			// 		Group          = x.Group,
			// 		PossibleValues = x.AllowedValues,
			// 		Value          = null //x.Value
			// 	}
			// );
				
			entity.ReplyTextContent(
				Codec.Json.To(options),
				HttpStatusCode.OK,
				"OK",
				entity.ResponseCodec.ContentType,
				null,
				e => Log.Error(e, "error while writing HTTP response (options)")
			);
		} else {
			entity.ReplyStatus(HttpStatusCode.Unauthorized, "Unauthorized", LogReplyError);
		}
	}

	void LogReplyError(Exception exc) => Log.Debug("Error while replying (info controller): {e}.", exc.Message);

	public class OptionStructure {
		public string   Name           { get; set; }
		public string   Description    { get; set; }
		public string   Group          { get; set; }
		public string   Value          { get; set; }
		public string[] PossibleValues { get; set; }
	}
}