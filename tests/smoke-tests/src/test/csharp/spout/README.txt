Interop.Spout

  dotnet run -- --broker amqp://10.20.30.40:5672 --address options --delay 100 --count 10000

Note:
 * Exit codes don't work
 * reply-to not implemented

For help:

  dotnet run -- --help

("Usage: Interop.Spout [OPTIONS] --address STRING");
("");
("Create a connection, attach a sender to an address, and send messages.");
("Options:");
(" --address STRING  []      - AMQP 1.0 terminus name");
(" --broker [amqp://guest:guest@127.0.0.1:5672] - AMQP 1.0 peer connection address");
(" --content STRING  []      - message content");
(" --count INT       [1]     - send this many messages and exit; 0 disables count based exit");
(" --delay MSECS     [0]     - delay between messages in milliseconds");
(" --durable         [false] - send messages marked as durable");
(" --id STRING       [guid]  - message id prefix");
(" --instances INT   [1]     - run this many instances of Spout");
(" --print           [false] - print each message's content");
(" --replyto STRING  []      - message ReplyTo address");
(" --synchronous     [false] - wait for peer to accept message before sending next");
(" --timeout SECONDS [0]     - send for N seconds; 0 disables timeout");
(" --help                    - print this message and exit");
("");


