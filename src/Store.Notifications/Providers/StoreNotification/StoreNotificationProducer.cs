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
using Energistics.DataAccess.WITSML141.ReferenceData;
using log4net;
using PDS.WITSMLstudio.Data.ChangeLogs;
using PDS.WITSMLstudio.Store.Configuration;

namespace PDS.WITSMLstudio.Store.Providers.StoreNotification
{
    /// <summary>
    /// A basic implementation of the <see cref="IStoreNotificationProducer"/> interface.
    /// </summary>
    /// <seealso cref="PDS.WITSMLstudio.Store.Providers.StoreNotification.IStoreNotificationProducer" />
    [Export(typeof(IStoreNotificationProducer))]
    [PartCreationPolicy(CreationPolicy.Shared)]
    public class StoreNotificationProducer : IStoreNotificationProducer
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(StoreNotificationProducer));
        private readonly IDictionary<string, string> _config;
        private readonly ISerializer<string> _keyDeserializer;
        private readonly ISerializer<string> _valueDeserializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="StoreNotificationProducer"/> class.
        /// </summary>
        public StoreNotificationProducer()
        {
            _keyDeserializer = Serializers.Utf8;
            _valueDeserializer = Serializers.Utf8;
            _config = new Dictionary<string, string>
            {
                {KafkaSettings.DebugKey, KafkaSettings.DebugContexts},
                {KafkaSettings.BrokerListKey, KafkaSettings.BrokerList}
            };
        }

        /// <summary>
        /// Sends the notification messages for the specified entity.
        /// </summary>
        /// <typeparam name="T">The data object type.</typeparam>
        /// <param name="entity">The changed entity.</param>
        /// <param name="auditHistory">The audit history.</param>
        public void SendNotifications<T>(T entity, DbAuditHistory auditHistory)
        {
            // No action if broker list not configured
            if (string.IsNullOrWhiteSpace(KafkaSettings.BrokerList)) return;

            var uri = auditHistory.Uri.ToLowerInvariant();
            var xml = WitsmlParser.ToXml(entity);

            var topic = auditHistory.LastChangeType == ChangeInfoType.delete
                ? KafkaSettings.DeleteTopicName
                : auditHistory.LastChangeType == ChangeInfoType.add
                    ? KafkaSettings.InsertTopicName
                    : KafkaSettings.UpdateTopicName;

            SendNotification(topic, uri, xml);

            // For backward compatibility with ETP v1.1
            if (auditHistory.LastChangeType != ChangeInfoType.delete)
            {
                SendNotification(KafkaSettings.UpsertTopicName, uri, xml);
            }
        }

        private void SendNotification(string topic, string uri, string xml)
        {
            Task.Run(() =>
            {
                try
                {
                    using (var producer = new ProducerBuilder<string, string>(_config)
                        .SetKeySerializer(_keyDeserializer)
                        .SetValueSerializer(_valueDeserializer)
                        .Build())
                    {
                        _log.Debug($"{producer.Name} producing on {topic}.");

                        var task = producer.ProduceAsync(topic, new Message<string, string>() { Key = uri, Value = xml });
                        var result = task.Result;

                        _log.Debug($"Partition: {result.Partition}; Offset: {result.Offset}");
                    }
                }
                catch (Exception ex)
                {
                    _log.Warn($"Error producing on topic: {topic}", ex);
                }
            });
        }
    }
}
