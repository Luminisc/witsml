//----------------------------------------------------------------------- 
// PDS WITSMLstudio Store, 2018.3
//
// Copyright 2018 PDS Americas LLC
// 
// Licensed under the PDS Open Source WITSML Product License Agreement (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
//     http://www.pds.group/WITSMLstudio/OpenSource/ProductLicenseAgreement
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Energistics.Etp.v11.Protocol.StoreNotification;
using PDS.WITSMLstudio.Framework;
using PDS.WITSMLstudio.Store.Configuration;

namespace PDS.WITSMLstudio.Store.Providers.StoreNotification
{
    /// <summary>
    /// Default implementation of a Store Notification Store provider.
    /// </summary>
    /// <seealso cref="StoreNotification11StoreProviderBase" />
    [Export(typeof(IStoreNotificationStore))]
    [PartCreationPolicy(CreationPolicy.NonShared)]
    public class StoreNotification11StoreProvider : StoreNotification11StoreProviderBase
    {
        private readonly IDictionary<string, string> _config;
        private readonly IDeserializer<string> _keyDeserializer;
        private readonly IDeserializer<string> _valueDeserializer;
        private readonly TimeSpan _timeout;
        private IConsumer<string, string> _consumer;
        private bool _isCancelled;

        /// <summary>
        /// Initializes a new instance of the <see cref="StoreNotification11StoreProvider"/> class.
        /// </summary>
        public StoreNotification11StoreProvider()
        {
            _timeout = TimeSpan.FromMilliseconds(KafkaSettings.PollingIntervalInMilliseconds);
            _keyDeserializer = Deserializers.Utf8;
            _valueDeserializer = Deserializers.Utf8;

            _config = new Dictionary<string, string>
            {
                {KafkaSettings.DebugKey, KafkaSettings.DebugContexts},
                {KafkaSettings.BrokerListKey, KafkaSettings.BrokerList},
                {KafkaSettings.EnableAutoCommitKey, KafkaSettings.EnableAutoCommit.ToString()}
            };
        }

        /// <summary>
        /// Ensures the connection to the message broker.
        /// </summary>
        protected override void EnsureConnection()
        {
            // No action if consumer already subscribed or broker list not configured
            if (_consumer != null || string.IsNullOrWhiteSpace(KafkaSettings.BrokerList)) return;

            // Set the group identifier
            _config[KafkaSettings.GroupIdKey] = Session.ApplicationName;

            // Create and configure a new Consumer instance
            _consumer = new ConsumerBuilder<string, string>(_config)
                .SetKeyDeserializer(_keyDeserializer)
                .SetValueDeserializer(_valueDeserializer)
                .SetPartitionsAssignedHandler((sender, partitions) =>
                {
                    Logger?.Debug($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {_consumer.MemberId}");
                    _consumer.Assign(partitions);
                })
                .SetPartitionsRevokedHandler((sender, partitions) =>
                {
                    Logger?.Warn($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    _consumer.Unassign();
                })
                .SetErrorHandler((sender, error) =>
                {
                    Logger?.Error($"Error: {error}");
                })
                .Build();

            _consumer.Subscribe(new[] {KafkaSettings.UpsertTopicName, KafkaSettings.DeleteTopicName});

            Task.Run(() =>
            {
                try
                {
                    while (!_isCancelled)
                    {
                        var message = _consumer.Consume();
                        OnMessage(this, message);
                    }
                }
                catch (Exception ex)
                {
                    Logger?.Warn("Error polling message broker", ex);
                }
            });
        }

        /// <summary>
        /// Disconnects from the message broker.
        /// </summary>
        protected override void Disconnect()
        {
            _isCancelled = true;
            _consumer?.Dispose();
            _consumer = null;
        }

        private void OnMessage(object sender, ConsumeResult<string, string> message)
        {
            Logger?.Debug($"Topic: {message.Topic}; Partition: {message.Partition}; Offset: {message.Offset}; {message.Message.Value}");

            // Extract message values
            var uri = message.Message.Key;
            var dataObject = message.Message.Value;
            var timestamp = DateTime.UtcNow;

            // Detect Upsert/Delete based on topic name
            if (KafkaSettings.DeleteTopicName.EqualsIgnoreCase(message.Topic))
            {
                OnNotifyDelete(uri, dataObject, timestamp);
            }
            else
            {
                OnNotifyUpsert(uri, dataObject, timestamp);
            }
        }
    }
}
