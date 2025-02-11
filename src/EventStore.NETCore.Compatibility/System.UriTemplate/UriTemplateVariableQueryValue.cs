// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

#pragma warning disable IDE0073 // The file header does not match the required text
//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//----------------------------------------------------------------

namespace System;

    using System.Collections.Specialized;
    using System.Runtime;
    using System.ServiceModel.Channels;
    using System.Text;

    class UriTemplateVariableQueryValue : UriTemplateQueryValue
    {
        readonly string varName;

        public UriTemplateVariableQueryValue(string varName)
            : base(UriTemplatePartType.Variable)
        {
            Fx.Assert(!string.IsNullOrEmpty(varName), "bad variable segment");
            this.varName = varName;
        }
        public override void Bind(string keyName, string[] values, ref int valueIndex, StringBuilder query)
        {
            Fx.Assert(valueIndex < values.Length, "Not enough values to bind");
            if (values[valueIndex] == null)
            {
                valueIndex++;
            }
            else
            {
                query.AppendFormat("&{0}={1}", UrlUtility.UrlEncode(keyName, Encoding.UTF8), UrlUtility.UrlEncode(values[valueIndex++], Encoding.UTF8));
            }
        }

        public override bool IsEquivalentTo(UriTemplateQueryValue other)
        {
            if (other == null)
            {
                Fx.Assert("why would we ever call this?");
                return false;
            }
            return (other.Nature == UriTemplatePartType.Variable);
        }
        public override void Lookup(string value, NameValueCollection boundParameters)
        {
            boundParameters.Add(this.varName, value);
        }
    }
