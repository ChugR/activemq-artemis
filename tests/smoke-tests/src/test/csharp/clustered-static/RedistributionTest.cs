/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using Amqp.Framing;
using Amqp.Extensions;
using Amqp.Sasl;
using Amqp.Types;
using Xunit;

namespace Amqp.Extensions.Examples
{
    public class RedistributionTest
    {
       // [Fact]
        public void testRedistribution()
        {
            // create receiver link on broker0
            Connection connection0 = new Connection(new Address("amqp://127.0.0.1:5672"));
            Session session0 = new Session(connection0);
            ReceiverLink receiver0 = new ReceiverLink(
                session0,
                "test|0",
                new Source()
                {
                    Address = "orders",
                    Capabilities = new Symbol[] { "topic", "shared", "global" },
                    Durable = 2,
                    ExpiryPolicy = new Symbol("never")
                },
                null);
            //receiver0.Detach();

            // send messages to broker0
            SenderLink sender = new SenderLink(session0, "sender", "orders");
            Message message = new Message("a message!");
            message.Properties = new Properties();
            message.Properties.To = "orders";

            for (var i = 0; i < 500; i++)
            {
                sender.Send(message);
            }

            // receive 1 of 5, works as expected...
            Message m = receiver0.Receive(TimeSpan.FromSeconds(1));
            Assert.NotNull(m);
            Console.WriteLine(m.Body);
            receiver0.Accept(m);
            session0.Close();
            connection0.Close();

            Connection connection1 = new Connection(new Address("amqp://127.0.0.1:15672"));
            Session session1 = new Session(connection1);

            // create a receiverlink emulating a shared durable subscriber by passing the capabilities below, and making it durable, and never expire
            ReceiverLink receiver1 = new ReceiverLink(
                session1,
                "test|1",
                new Source()
                {
                    Address = "orders",
                    Capabilities = new Symbol[] { "topic", "shared", "global" },
                    Durable = 1,
                    ExpiryPolicy = new Symbol("never")
                },
                null);


            // these 4 messages are removed from broker0 (ack'd) but never delivered. NPE seen in logs on broker1
            for (var i = 0; i < 499; i++)
            {
                m = receiver1.Receive(TimeSpan.FromSeconds(1));
                Assert.NotNull(m);
                if (m == null)
                {
                    Console.WriteLine("It's null");
                }
                Console.WriteLine("Received " + m);
            }

            Assert.Null(receiver1.Receive(TimeSpan.FromSeconds(1)));

            sender.Close();
            receiver0.Detach();
            receiver1.Detach();
            session0.Close();
            session1.Close();
            connection0.Close();
            connection1.Close();
        }

         [Fact]
         public void testExpireDistributed()
         {
             // create receiver link on broker0
             Connection connection0 = new Connection(new Address("amqp://127.0.0.1:5672"));
             Session session0 = new Session(connection0);
            ReceiverLink receiver0 = new ReceiverLink(
                session0,
                "test|0",
                new Source()
                {
                    Address = "orders",
                    Capabilities = new Symbol[] { "topic", "shared", "global" },
                    Durable = 2,
                    ExpiryPolicy = new Symbol("never")
                },
                null);
             // receiver0.Detach();

             // send messages to broker0
             SenderLink sender = new SenderLink(session0, "sender", "orders");
             Message message = new Message("a message!");
             message.Properties = new Properties();
             message.Properties.To = "orders";
             message.ApplicationProperties = new ApplicationProperties();
             message.ApplicationProperties.Map.Add("hello", "world");
             message.Header = new Header();
             message.Header.Durable=true;
             message.Header.Ttl = 1000;
            
             for (var i = 0; i < 500; i++)
             {
                 Console.WriteLine("sending " + i);
                 sender.Send(message);
             }

             // Some time to let messages expire
             System.Threading.Thread.Sleep(5000);

             session0.Close();
             connection0.Close();

             Console.WriteLine("done");

            Connection connection1 = new Connection(new Address("amqp://127.0.0.1:15672"));
            Session session1 = new Session(connection1);

            // create a receiverlink emulating a shared durable subscriber by passing the capabilities below, and making it durable, and never expire
            ReceiverLink receiver1 = new ReceiverLink(
                session1,
                "test|1",
                new Source()
                {
                    Address = "ExpiryQueue" ,
                    Capabilities = new Symbol[] { "queue"}
                },
                null);


            for (int i = 0; i < 500; i++) {
                // receive 1 of 5, works as expected...
                Message m = receiver1.Receive(TimeSpan.FromSeconds(10));
                Assert.NotNull(m);
                Console.WriteLine("Received " + i);
            }
            
             //receiver0.Accept(m);
             session0.Close();
             connection0.Close();

         }

         // [Fact]
        public void testLocal()
        {
            // create receiver link on broker0
            Connection connection0 = new Connection(new Address("amqp://127.0.0.1:5672"));
            Session session0 = new Session(connection0);
            ReceiverLink receiver0 = new ReceiverLink(
                session0,
                "test|0",
                new Source()
                {
                    Address = "orders",
                    Capabilities = new Symbol[] { "topic", "shared", "global" },
                    Durable = 2,
                    ExpiryPolicy = new Symbol("never")
                },
                null);
            //receiver0.Detach();

            // send messages to broker0
            SenderLink sender = new SenderLink(session0, "sender", "orders");
            Message message = new Message("a message!");
            message.Properties = new Properties();
            message.Properties.To = "orders";

            for (var i = 0; i < 500; i++)
            {
                sender.Send(message);
            }

            // receive 1 of 5, works as expected...
            Message m = receiver0.Receive(TimeSpan.FromSeconds(1));
            Assert.NotNull(m);
            Console.WriteLine(m.Body);
            receiver0.Accept(m);

            // create a receiverlink emulating a shared durable subscriber by passing the capabilities below, and making it durable, and never expire
            ReceiverLink receiver1 = receiver0;


            // these 4 messages are removed from broker0 (ack'd) but never delivered. NPE seen in logs on broker1
            for (var i = 0; i < 499; i++)
            {
                m = receiver1.Receive(TimeSpan.FromSeconds(1));
                Assert.NotNull(m);
                if (m == null)
                {
                    Console.WriteLine("It's null");
                }
                Console.WriteLine("Received " + m);
            }

            Assert.Null(receiver1.Receive(TimeSpan.FromSeconds(1)));

            sender.Close();
            receiver0.Detach();
            receiver1.Detach();
            session0.Close();
            connection0.Close();
        }

    }

}
