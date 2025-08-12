using System.Collections.Generic;
using System.Net;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;

// AI code, presumably not working

namespace Energistics.DataAccess.ClientBaseBehaviors
{
    public class WebProxyBehavior : IEndpointBehavior
    {
        private readonly WebProxy _proxy;

        public WebProxyBehavior(WebProxy proxy)
        {
            _proxy = proxy;
        }

        public void AddBindingParameters(ServiceEndpoint endpoint, BindingParameterCollection bindingParameters)
        {
            bindingParameters.Add(new WebProxyWrapper(_proxy));
        }

        public void ApplyClientBehavior(ServiceEndpoint endpoint, ClientRuntime clientRuntime) { }
        public void ApplyDispatchBehavior(ServiceEndpoint endpoint, EndpointDispatcher endpointDispatcher) { }
        public void Validate(ServiceEndpoint endpoint) { }
    }

    public class WebProxyWrapper
    {
        public WebProxy Proxy { get; private set; }

        public WebProxyWrapper(WebProxy proxy)
        {
            Proxy = proxy;
        }
    }

    public class PreAuthenticationBehavior : IEndpointBehavior
    {
        public void AddBindingParameters(ServiceEndpoint endpoint, BindingParameterCollection bindingParameters)
        {
            bindingParameters.Add(new PreAuthenticationMessageInspector());
        }

        public void ApplyClientBehavior(ServiceEndpoint endpoint, ClientRuntime clientRuntime)
        {
            clientRuntime.ClientMessageInspectors.Add(new PreAuthenticationMessageInspector());
        }

        public void ApplyDispatchBehavior(ServiceEndpoint endpoint, EndpointDispatcher endpointDispatcher) { }
        public void Validate(ServiceEndpoint endpoint) { }
    }

    public class PreAuthenticationMessageInspector : IClientMessageInspector
    {
        public void AfterReceiveReply(ref Message reply, object correlationState) { }

        public object BeforeSendRequest(ref Message request, IClientChannel channel)
        {
            HttpRequestMessageProperty httpRequestProperty;

            if (request.Properties.TryGetValue(HttpRequestMessageProperty.Name, out var httpRequestPropertyObj))
            {
                httpRequestProperty = (HttpRequestMessageProperty)httpRequestPropertyObj;
            }
            else
            {
                httpRequestProperty = new HttpRequestMessageProperty();
                request.Properties.Add(HttpRequestMessageProperty.Name, httpRequestProperty);
            }

            httpRequestProperty.Headers[HttpRequestHeader.KeepAlive] = "true";

            return null;
        }
    }

    public class CustomHeadersBehavior : IEndpointBehavior
    {
        private readonly IDictionary<string, string> _headers;

        public CustomHeadersBehavior(IDictionary<string, string> headers)
        {
            _headers = headers;
        }

        public void AddBindingParameters(ServiceEndpoint endpoint, BindingParameterCollection bindingParameters) { }

        public void ApplyClientBehavior(ServiceEndpoint endpoint, ClientRuntime clientRuntime)
        {
            clientRuntime.ClientMessageInspectors.Add(new CustomHeadersMessageInspector(_headers));
        }

        public void ApplyDispatchBehavior(ServiceEndpoint endpoint, EndpointDispatcher endpointDispatcher) { }
        public void Validate(ServiceEndpoint endpoint) { }
    }

    public class CustomHeadersMessageInspector : IClientMessageInspector
    {
        private readonly IDictionary<string, string> _headers;

        public CustomHeadersMessageInspector(IDictionary<string, string> headers)
        {
            _headers = headers;
        }

        public void AfterReceiveReply(ref Message reply, object correlationState) { }

        public object BeforeSendRequest(ref Message request, IClientChannel channel)
        {
            HttpRequestMessageProperty httpRequestProperty;

            if (request.Properties.TryGetValue(HttpRequestMessageProperty.Name, out var httpRequestPropertyObj))
            {
                httpRequestProperty = (HttpRequestMessageProperty)httpRequestPropertyObj;
            }
            else
            {
                httpRequestProperty = new HttpRequestMessageProperty();
                request.Properties.Add(HttpRequestMessageProperty.Name, httpRequestProperty);
            }

            foreach (string key in _headers.Keys)
            {
                httpRequestProperty.Headers[key] = _headers[key];
            }

            return null;
        }
    }
}
