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

package org.apache.activemq.artemis.tests.smoke.clusteredstatic;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedistributionTest extends SmokeTestBase {

   private static final String SERVER_NAME_0 = "clustered-static-node0";
   private static final String SERVER_NAME_1 = "clustered-static-node1";
   private static final String SERVER_NAME_2 = "clustered-static-node2";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      cleanupData(SERVER_NAME_2);
      startServer(SERVER_NAME_0, 0, 30000);
      startServer(SERVER_NAME_1, 1, 30000);
      //startServer(SERVER_NAME_2, 2, 30000);
   }

   @Test
   public void runServers() throws Exception {
      Thread.sleep(50000);
   }

   @Test
   public void testCSharpEquivalen() throws Exception {

      JmsConnectionFactory client0 = new JmsConnectionFactory("amqp://127.0.0.1:5672");
      JmsConnectionFactory client1 = new JmsConnectionFactory("amqp://127.0.0.1:5673");

      // create receiver link on broker0
      Connection connection0 = client0.createConnection();
      connection0.setClientID("topic1");
      Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = session0.createTopic("orders");

      MessageConsumer receiver0 = session0.createDurableSubscriber(topic, "shared");

      // send messages to broker0
      MessageProducer produce0 = session0.createProducer(topic);

      for (int i = 0; i < 5; i++) {
         produce0.send(session0.createTextMessage("tp"));
      }

      connection0.start();

      TextMessage m = (TextMessage) receiver0.receive(500);
      Assert.assertNotNull(m);
      connection0.close();

      Connection connection1 = client1.createConnection();
      connection1.setClientID("topic1");
      connection1.start();
      Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer1 = session1.createDurableSubscriber(topic, "shared");

      for (int i = 0; i < 4; i++) {
         m = (TextMessage) consumer1.receive(5000);
         Assert.assertNotNull(m);
      }

      connection1.close();
   }

   @Test
   public void testTraverseMessage() throws Exception {

      JmsConnectionFactory client0 = new JmsConnectionFactory("amqp://127.0.0.1:5672");
      JmsConnectionFactory client1 = new JmsConnectionFactory("amqp://127.0.0.1:5673");
      Connection connection0 = client0.createConnection();
      Connection connection1 = client1.createConnection();
      connection1.setClientID("sub1");
      connection1.start();
      Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      connection0.setClientID("conn1");
      Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = session0.createTopic("orders");
      connection0.start();

      MessageConsumer consumer1 = session1.createDurableSubscriber(topic, "sub1");
      MessageProducer producer1 = session1.createProducer(topic);

      MessageConsumer consumer0 = session0.createDurableSubscriber(topic, "sub0");
      MessageProducer producer0 = session0.createProducer(topic);

      for (int i = 0; i < 5; i++) {
         producer0.send(session0.createTextMessage("hello " + i));
         producer1.send(session0.createTextMessage("hello " + i));
      }

      for (int i = 0; i < 10; i++) {
         Assert.assertNotNull(consumer0.receive(5000));
         Assert.assertNotNull(consumer1.receive(5000));
      }

      connection0.close();
   }
}
