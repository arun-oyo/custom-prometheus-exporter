import subprocess
import concurrent.futures
import time

COOL_DOWN_PERIOD_SECONDS = 60

BASE_PATH = "/Users/oyo"

FILE_PREFIX = BASE_PATH + "/kafka-exporter/files/consumer-lag-"
MERTICS_FILE = BASE_PATH + "/kafka-exporter/files/metrics"


######################################################################################
#PROD
SERVICE = "prod"

BOOTSTRAP_SERVERS = []

######################################################################################


NUM_OF_BATCHES = 50

def sanitize_line(line):
    line = line.replace('\n','')
    line = line.replace('\r','')
    line = line.replace('\b','')
    line = line.replace('\f','')
    line = line.replace('\t','')
    line = line.strip()
    return line


# Define an asynchronous function
def fetch_from_kafka_server(consumer_groups_list, filename, index):
    subprocess.run(['rm', filename])
    for group in consumer_groups_list:
        if not group or group.strip() == "":
            continue
        print(filename + " ----- running for: " + group)
        result = subprocess.run("/Users/oyo/Downloads/kafka_2.10-0.10.0.0/bin/kafka-consumer-groups.sh --bootstrap-server " + BOOTSTRAP_SERVERS[index % len(BOOTSTRAP_SERVERS)] + " --describe --group " + group + " --new-consumer", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            result = result.stdout
            if "TOPIC" in result:
                header_last_index = result.find('\n')
                result = result[header_last_index + 1:]
                with open(filename, "a+") as file:
                    file.write(result)


# Define the main entry point
def main():

    while True:

        try:

            consumer_groups_out = subprocess.run("/Users/oyo/Downloads/kafka_2.11-0.11.0.1/bin/kafka-consumer-groups.sh --bootstrap-server " + BOOTSTRAP_SERVERS[0] + " --list", shell=True, capture_output=True, text=True)

            if consumer_groups_out.returncode == 0:
                print("Command executed successfully!")
                consumer_grps_list = consumer_groups_out.stdout.split('\n')

                if consumer_grps_list[-1] == "":
                    consumer_grps_list.pop()

                print(consumer_grps_list)
            
                batch_size = int(len(consumer_grps_list) // NUM_OF_BATCHES) + 1

                batches = [consumer_grps_list[i:i+batch_size] for i in range(0, len(consumer_grps_list), batch_size)]

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = []
                    for i in range(len(batches)):
                        future = executor.submit(fetch_from_kafka_server, batches[i], FILE_PREFIX + str(i), i)
                        futures.append(future)

                    concurrent.futures.wait(futures)

                # build the prometheus metrics
                lines = []
                for index in range(len(batches)):
                    filename = FILE_PREFIX + str(index)
                    try:
                        with open(filename, "r") as file:
                            next(file)
                            for line in file:
                                line = sanitize_line(line)
                                lines.append(line)
                    except IOError:
                        print("file does not exist: " + filename)
                        pass

                data = [line.split() for line in lines]

                metrics = []
                for item in data:
                    if item[5] != "unknown":
                        metric = 'kafka_custom_metrics_consumer_lag{service="' + SERVICE + '", group="' + item[0] + '", topic="' + item[1] + '", partition="' + item[2] + '"} ' + item[5]
                        metrics.append(metric)

                prometheus_metrics = '\n'.join(metrics)

                with open(MERTICS_FILE, "w+") as file:
                    file.write(prometheus_metrics)

                for i in range(len(batches)):
                    subprocess.run(['rm', FILE_PREFIX + str(i)])

        except:
            pass

        time.sleep(COOL_DOWN_PERIOD_SECONDS)

main()
