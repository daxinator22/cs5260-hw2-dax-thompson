from sys import argv, exit
import Client

if len(argv) < 4:
    print('Usage: consumer.py from_bucket method to_bucket')
else:
    client = Client.Client(argv[1], argv[2], argv[3])
    while True:
        try:
            client.process_next_request()

        except KeyboardInterrupt:
            print('consumer.py interrupted by user')
            exit()
