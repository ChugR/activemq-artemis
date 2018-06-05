//  ------------------------------------------------------------------------------------
//  Copyright (c) Microsoft Corporation
//  All rights reserved. 
//  
//  Licensed under the Apache License, Version 2.0 (the ""License""); you may not use this 
//  file except in compliance with the License. You may obtain a copy of the License at 
//  http://www.apache.org/licenses/LICENSE-2.0  
//  
//  THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
//  EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR 
//  CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR 
//  NON-INFRINGEMENT. 
// 
//  See the Apache Version 2.0 License for specific language governing permissions and 
//  limitations under the License.
//  ------------------------------------------------------------------------------------

namespace Examples.Interop
{
    using System;

    public class Options
    {
        private string address;
        private string content;
        private int    count;
        private int    delay;
        private bool   durable;
        private string id;
        private int    instances;
        private bool   print;
        private string replyto;
        private bool   synchronous;
        private int    timeout;
        private string url;

        public Options(string[] args)
        {
            this.address = "";
            this.content = "";
            this.count = 1;
            this.delay = 0;
            this.durable = false;
            this.id = "";
            this.instances = 1;
            this.print = false;
            this.replyto = "";
            this.synchronous = false;
            this.timeout = 0;
            this.url = "amqp://guest:guest@127.0.0.1:5672";
            Parse(args);
        }

        private void Usage()
        {
            Console.WriteLine("Usage: Interop.Spout [OPTIONS] --address STRING");
            Console.WriteLine("");
            Console.WriteLine("Create a connection, attach a sender to an address, and send messages.");
            Console.WriteLine("Options:");
            Console.WriteLine(" --address STRING  []      - AMQP 1.0 terminus name");
            Console.WriteLine(" --broker [amqp://guest:guest@127.0.0.1:5672] - AMQP 1.0 peer connection address");
            Console.WriteLine(" --content STRING  []      - message content");
            Console.WriteLine(" --count INT       [1]     - send this many messages and exit; 0 disables count based exit");
            Console.WriteLine(" --delay MSECS     [0]     - delay between messages in milliseconds");
            Console.WriteLine(" --durable         [false] - send messages marked as durable");
            Console.WriteLine(" --id STRING       [guid]  - message id prefix");
            Console.WriteLine(" --instances INT   [1]     - run this many instances of Spout");
            Console.WriteLine(" --print           [false] - print each message's content");
            Console.WriteLine(" --replyto STRING  []      - message ReplyTo address");
            Console.WriteLine(" --synchronous     [false] - wait for peer to accept message before sending next");
            Console.WriteLine(" --timeout SECONDS [0]     - send for N seconds; 0 disables timeout");
            Console.WriteLine(" --help                    - print this message and exit");
            Console.WriteLine("");
            Console.WriteLine("Exit codes:");
            Console.WriteLine(" 0 - successfully received all messages");
            Console.WriteLine(" 2 - other error");
            System.Environment.Exit(2);
        }

        private void Parse(string[] args)
        {
            int argCount = args.Length;
            int current = 0;

            while (current < argCount)
            {
                string arg = args[current];
                if (arg == "--help")
                {
                    Usage();
                }
                else if (arg == "--broker")
                {
                    this.url = args[++current];
                }
                else if (arg == "--address")
                {
                    this.address = args[++current];
                }
                else if (arg == "--timeout")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.timeout = i;
                    }
                }
                else if (arg == "--durable")
                {
                    this.durable = true;
                }
                else if (arg == "--count")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i >= 0)
                    {
                        this.count = i;
                    }
                }
                else if (arg == "--id")
                {
                    this.id = args[++current];
                }
                else if (arg == "--replyto")
                {
                    this.replyto = args[++current];
                }
                else if (arg == "--content")
                {
                    this.content = args[++current];
                }
                else if (arg == "--print")
                {
                    this.print = true;
                }
                else if (arg == "--delay")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i>0)
                    {
                        this.delay = i;
                    }
                }
                else if (arg == "--instances")
                {
                    arg = args[++current];
                    int i = int.Parse(arg);
                    if (i > 0)
                    {
                        this.instances = i;
                    }
                }
                else if (arg == "--synchronous")
                {
                    this.synchronous = true;
                }
                else
                {
                    throw new ArgumentException(String.Format("unknown argument \"{0}\"", arg));
                }

                current++;
            }

            if (this.address.Equals(""))
            {
                throw new ArgumentException("missing argument: --address");
            }
        }

        public string Url
        {
            get { return this.url; }
        }

        public string Address
        {
            get { return this.address; }
        }

        public int Timeout
        {
            get { return this.timeout; }
        }

        public bool Durable
        {
            get { return this.durable; }
        }

        public int Count
        {
            get { return this.count; }
        }

        public string Id
        {
            get { return this.id; }
        }

        public string ReplyTo
        {
            get { return this.replyto; }
        }

        public string Content
        {
            get { return this.content; }
        }

        public bool Print
        {
            get { return this.print; }
        }

        public int Delay
        {
            get { return this.delay; }
        }

        public int Instances
        {
            get { return this.instances; }
        }

        public bool Synchronous
        {
            get { return this.synchronous;  }
        }
    }
}
