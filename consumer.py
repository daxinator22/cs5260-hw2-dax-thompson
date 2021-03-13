from sys import argv
import Client

if len(argv) < 4:
    print('Usage: consumer.py from_bucket method to_bucket')
else:
    client = Client.Client(argv[1], argv[2], argv[3])
    while True:
        client.process_next_request()
