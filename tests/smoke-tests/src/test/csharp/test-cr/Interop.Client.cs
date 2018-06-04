//#define CLIENT_MANAGES_CREDIT
#define LITE_MANAGES_CREDIT

// Test3
// Send and receive some messages to an address.
// This code is used to test credit between this client and the server.
//
// The general flow is:
//   1. Send a block of N unsettled, durable messages to the server
//   2. Open a receiver with enough credit to receive only some of the messages
//   3. As messages are received, wait some variable number of seconds for each message
//      and then acept the message. Messages are accepted out of order.
//   4. Code waits for a long time for all messages.
//
// Command line:
//  test3 address                         N_to_send receiver_credit
//
//  test3 amqp://10.3.116.118:5672/orders 200       20
//
using System;
using System.Threading;
using Amqp.Framing;
using System.Threading.Tasks;

namespace Amqp.Extensions.Examples
{
    class Program
    {
        static void Main(string[] args)
        {
            string addr  = args.Length >= 1 ? args[0] : "amqp://10.3.116.118:5672/orders";
            int sendN = args.Length >= 2 ? Convert.ToInt32(args[1]) : 200;
            int credit = args.Length >= 3 ? Convert.ToInt32(args[2]) : 20;

            Address address = new Address(addr);
            string queue = address.Path.Substring(1);

            Random rnd = new Random();

            Connection connection = new Connection(address);
            Session session = new Session(connection);
            ReceiverLink receiver = new ReceiverLink(session, "receiver", queue);

            // sleep to make sure the queue is created before we send, we could await the attach frame but this is just a demo.
            Thread.Sleep(1000);

            SenderLink sender = new SenderLink(session, "sender", queue);
            Message message = new Message("a message!");
            message.Header = new Header();
            message.Header.Durable = true;
            OutcomeCallback callback = (l, msg, o, s) => { };

            Console.WriteLine("Sending {0} messages...", sendN);
            for (var i = 0; i < sendN; i++)
            {
                sender.Send(message, callback, null);
            }
            Console.WriteLine(".... Done sending");

            Trace.TraceLevel = TraceLevel.Verbose | TraceLevel.Error |
            TraceLevel.Frame | TraceLevel.Information | TraceLevel.Warning;
            Trace.TraceListener = (l, f, o) => Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, o));

            sender.Close();

#if CLIENT_MANAGES_CREDIT
            int nInProgress = 0;
            int msgsRetired = 0;
            const int creditEveryN = 5;
            const int credit = 10;
            int msgsRetired = 0;

            receiver.Start(0, async (r, m) =>
            {
                int depth;
                int nRetired;
                depth = Interlocked.Increment(ref nInProgress);
                double delay = System.Convert.ToDouble(rnd.Next(2, 20));
                Console.WriteLine("Depth= {0} In receive callback. Starting await of {1} seconds", depth, delay);
                await Task.Delay(TimeSpan.FromSeconds(delay));
                r.Accept(m);
                depth = Interlocked.Decrement(ref nInProgress);
                Console.WriteLine("Depth= {0} Exiting.  Accepting message", depth);
                nRetired = Interlocked.Increment(ref msgsRetired);
                if (nRetired % creditEveryN == 0)
                {
                    receiver.SetCredit(credit - depth, false);
                }
            });
            receiver.SetCredit(credit, false);
#endif

#if LITE_MANAGES_CREDIT
            int nIn = 0;
            int nDone = 0;
            receiver.Start(credit, async (r, m) =>
            {
                nIn++;
                Console.WriteLine("nIn = {0}, nDone = {1}, InFlight = {2} In receive callback. Starting await...", nIn, nDone, nIn-nDone);
                double delay = System.Convert.ToDouble(rnd.Next(2, 20));
                await Task.Delay(TimeSpan.FromSeconds(delay));
                r.Accept(m);
                nDone++;
                Console.WriteLine("nIn = {0}, nDone = {1}, InFlight = {2} ... Exiting Task.Delay", nIn, nDone, nIn - nDone);
            });
#endif
            Thread.Sleep(3000000);
        }
    }
}
