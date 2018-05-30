/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.cluster.crossprotocol;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.MessageFlowRecord;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class RedistributionMessTest extends ClusterTestBase {

   private static final int NUMBER_OF_SERVERS = 3;
   private static final SimpleString queueName = SimpleString.toSimpleString("queues.0");

   // I'm taking any number that /2 = Odd
   // to avoid perfect roundings and making sure messages are evenly distributed
   private static final int NUMBER_OF_MESSAGES = 10 * 2;

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{"AMQP"}, {"CORE"}});
   }

   @Parameterized.Parameter(0)
   public String protocol;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

   }

   private void startServers(MessageLoadBalancingType loadBalancingType) throws Exception {
      setupServers();

      setRedistributionDelay(0);

      setupCluster(loadBalancingType);

      AddressSettings as = new AddressSettings().setRedistributionDelay(0).setExpiryAddress(SimpleString.toSimpleString("queues.expiry"));

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);

      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         startServers(i);
      }

      createQueue(SimpleString.toSimpleString("queues.expiry"));
      createQueue(queueName);
   }

   private void createQueue(SimpleString queueName) throws Exception {
      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         servers[i].createQueue(queueName, RoutingType.ANYCAST, queueName, (SimpleString) null, (SimpleString) null, true, false, false, false, false, -1, false, false, false, true);
      }
   }

   protected boolean isNetty() {
      return true;
   }

   private ConnectionFactory getJmsConnectionFactory(int node) {
      if (protocol.equals("AMQP")) {
         return new JmsConnectionFactory("amqp://localhost:" + (61616 + node));
      } else {
         return new ActiveMQConnectionFactory("tcp://localhost:" + (61616 + node));
      }
   }

   private void pauseClusteringBridges(ActiveMQServer server) throws Exception {
      for (ClusterConnection clusterConnection : server.getClusterManager().getClusterConnections()) {
         for (MessageFlowRecord record : ((ClusterConnectionImpl) clusterConnection).getRecords().values()) {
            record.getBridge().pause();
         }
      }
   }

   @Test
   public void testRedistributeMess() throws Exception {

      startServers(MessageLoadBalancingType.ON_DEMAND);

      ConnectionFactory factory[] = new ConnectionFactory[NUMBER_OF_SERVERS];
      Connection connection[] = new Connection[NUMBER_OF_SERVERS];
      Session session[] = new Session[NUMBER_OF_SERVERS];
      MessageConsumer consumer[] = new MessageConsumer[NUMBER_OF_SERVERS];

      // this will pre create consumers to make sure messages are distributed evenly without redistribution
      // it will only create consumers on the first 2 nodes
      for (int node = 0; node < 2; node++) {
         factory[node] = getJmsConnectionFactory(node);
         connection[node] = factory[node].createConnection();
         session[node] = connection[node].createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer[node] = session[node].createConsumer(session[node].createQueue(queueName.toString()));
      }

      waitForBindings(0, "queues.0", 1, 1, true);
      waitForBindings(1, "queues.0", 1, 1, true);
      waitForBindings(2, "queues.0", 1, 0, true);

      waitForBindings(0, "queues.0", 2, 1, false);
      waitForBindings(1, "queues.0", 2, 1, false);
      waitForBindings(2, "queues.0", 2, 2, false);

      pauseClusteringBridges(servers[0]);

      // sending Messages.. they should be load balanced
      {
         ConnectionFactory cf = getJmsConnectionFactory(2);
         Connection cn = cf.createConnection();
         Session sn = cn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer pd = sn.createProducer(sn.createQueue(queueName.toString()));

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            pd.send(sn.createTextMessage("hello " + i));
         }

         cn.close();
      }


      receiveMessages(connection[0], consumer[0], NUMBER_OF_MESSAGES / 2, true);
      receiveMessages(connection[1], consumer[1], NUMBER_OF_MESSAGES / 2 - 2, false); // we will leave 2 messages out


      consumer[0].close();
      consumer[1].close();


      Thread.sleep(60000);


   }

   private void receiveMessages(Connection connection,
                                MessageConsumer messageConsumer,
                                int messageCount, boolean testLast) throws JMSException {
      connection.start();

      for (int i = 0; i < messageCount; i++) {
         Message msg = messageConsumer.receive(5000);
         Assert.assertNotNull("expected messaged at i = " + i, msg);
      }

      if (testLast) Assert.assertNull(messageConsumer.receiveNoWait());
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 2);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 0);
      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 2);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 2, 0);
      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 2, 1);
   }

   protected void setRedistributionDelay(final long delay) {
   }

   protected void setupServers() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      servers[0].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
      servers[1].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
      servers[2].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
   }

   protected void stopServers() throws Exception {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1, 2);

      clearServer(0, 1, 2);
   }

   /**
    * @param serverID
    * @return
    * @throws Exception
    */
   @Override
   protected ConfigurationImpl createBasicConfig(final int serverID) {
      ConfigurationImpl configuration = super.createBasicConfig(serverID);
      configuration.setMessageExpiryScanPeriod(100);

      return configuration;
   }

}
