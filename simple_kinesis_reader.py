import time, sys, boto3, argparse

def commandline():

    parser = argparse.ArgumentParser(
        description='This is a simple reader that pulls and prints messages from a kinesis stream.')

    parser.add_argument('stream_name', action='store', help='kinesis stream name')

    parser.add_argument('profile_name', action='store', help='name of the aws profile to use')

    parser.add_argument('--limit', action='store', dest='limit', default=10, type=int,
                        help='limit the number of records fetched with each read. Default 10')

    parser.add_argument('--sleep', action='store', dest='sleep', default=1.0, type=float,
                        help='sleep time to avoid rate limit errors in seconds. Default 1.0s')

    parser.add_argument('--filter', action='store', dest='simple_filter',
                        help='Will check if the record contains this string and then print it out. Default : not filtered.')

    parser.add_argument('--iterator', action='store', dest='shard_iterator_type', default="TRIM_HORIZON",
                        help='Shard iterator type, default : TRIM_HORIZON')

    parser.add_argument('--version', action='version', version='%(prog)s 1.0')

    return parser.parse_args()

def read_kinesis(properties):

    session = boto3.Session(profile_name=properties.profile_name)

    kinesis = session.client('kinesis')

    description = kinesis.describe_stream(StreamName=properties.stream_name)

    for shard in description['StreamDescription']['Shards']:

        pre_shard_it = kinesis.get_shard_iterator(StreamName=properties.stream_name, ShardId=shard['ShardId'],
                                                  ShardIteratorType=properties.shard_iterator_type)

        shard_it = pre_shard_it["ShardIterator"]

        notDone = True

        while notDone:
            records = kinesis.get_records(ShardIterator=shard_it, Limit=properties.limit)

            shard_it = records["NextShardIterator"]

            for record in records["Records"]:

                if properties.simple_filter == None or properties.simple_filter in record["Data"]:
                    print(record["Data"])

            time.sleep(properties.sleep)

            if records["MillisBehindLatest"] == 0:
                notDone = False

def main():
    command_line = commandline()
    read_kinesis(command_line)

if __name__ == "__main__":
    main()
