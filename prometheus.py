import os,stat
from time import sleep
from metric_buckets import buckets
from collections import OrderedDict

service = service = os.uname()[1]

# Example:
# metrics_data_map = {
#     http_requests_ms_total : {
#         "uri=jsonrpc-intern/insertrentalcontractv2.p;channel=website;" = [count, time_taken]
#     }
# }
metrics_data_map = {}

# Example:
# buckets_data_map = {
#      http_requests_ms_total : {
#          website : {
#                 insertrentalcontractv2 : {
#                       "250": 0,
#                       "500": 2        
#                 }
#             }
#      }
#  }
buckets_data_map = {}

metrics_file_path = "/root/prometheus/data/"
#metrics_file_path = "/tmp/"
metrics_file = metrics_file_path + "metrics_data"

log_file_path = "/appl/log/metrics/"
#log_file_path = "/Users/oyo/python-server/"
log_file = log_file_path + "application_metrics.log"
#log_file = log_file_path + "server.log"

def main():
    gather_metrics()


def get_error_metrics():
    metric_str = ""
    try:
        f = open('/root/prometheus/overall_error_count_metric', 'r')
        for metric_data in f:
            metric_data = metric_data.replace('\n', '')
            metric_data = metric_data.replace('\r', '')
            labels_list = metric_data.split(";")

            if not labels_list:
                continue

            metric_name = ""
            count = 0.0

            metric_map = {}
            for item in labels_list:
                key_value = item.split('=')
                if len(key_value) != 2:
                    break

                if key_value[0] == 'metric':
                    metric_name = key_value[1]
                    continue

                if key_value[0] == 'count':
                    count = float(key_value[1])
                    continue

                metric_map[key_value[0]] = key_value[1]


            if metric_map and metric_name:
                metric_str += metric_name + "{"
                for k,v in metric_map.items():
                    metric_str += k + "=" + "\"" + v + "\"" + ","

                metric_str += "} " + str(count) + "\n"
        f.close()
    except IOError:
        pass
    return metric_str


def prometheus():
    f = open(metrics_file,"r")
    data = f.read()
    f.close()
    err_metric = get_error_metrics()
    if err_metric:
        data = data + err_metric
    return data


def get_label_format(label, value):
    if label is None or value is None:
        return ""
    return label + "=" + "\"" + value + "\""


def is_bucketing_allowed(metric, channel, uri):
    return metric in buckets and channel in buckets[metric] and uri in buckets[metric][channel] and len(buckets[metric][channel][uri]) != 0

def create_buckets_data_with_fallback(metric, channel, uri):
    if metric not in buckets_data_map:
        buckets_data_map[metric] = {}

    if channel not in buckets_data_map[metric]:
        buckets_data_map[metric][channel] = {}

    if uri not in buckets_data_map[metric][channel]:
        buckets_data_map[metric][channel][uri] = OrderedDict()

    rebalance_required = False
    for le in buckets[metric][channel][uri]:
        if le not in buckets_data_map[metric][channel][uri]:
            rebalance_required = True
            break

    if rebalance_required:
        buckets_data_map[metric][channel][uri] = OrderedDict()
        for le in buckets[metric][channel][uri]:
            buckets_data_map[metric][channel][uri][le] = 0.0


def create_buckets_data(metric, channel, uri, time_taken):
    if metric is None or channel is None or uri is None or time_taken is None:
        return

    if not is_bucketing_allowed(metric, channel, uri):
        return
    
    create_buckets_data_with_fallback(metric, channel, uri)

    if time_taken == 0.0:
        return

    for le in buckets[metric][channel][uri]:
        if time_taken < float(le):
            buckets_data_map[metric][channel][uri][le] += 1


def create_metrics_data(metric, keys_data, time_taken):   
    if metric not in  metrics_data_map:
        metrics_data_map[metric] = {}

    metric_data = metrics_data_map[metric]
    if keys_data not in metric_data:
        metrics_data_map[metric][keys_data] = [1.0, time_taken]
        return

    metrics_data_map[metric][keys_data][0] += 1
    if time_taken is not None:
        if metrics_data_map[metric][keys_data][1] is None:
            metrics_data_map[metric][keys_data][1] = 0.0
        metrics_data_map[metric][keys_data][1] += time_taken



def buckets_data_string(metrics_str):
    if metrics_str is None:
        metrics_str = ""
    for metric, metric_data in buckets_data_map.items():
        for channel, channel_data in metric_data.items():
            for uri, uri_data in channel_data.items():
                for le, count in uri_data.items():
                    metrics_str += metric + "_bucket" + "{" + get_label_format("app_service", service) + "," + get_label_format("channel", channel) + "," + get_label_format("uri", uri) + "," + get_label_format("le", le) + "," + "} " + str(count) + "\n"
    return metrics_str


def write_metrics():
    metric_str = ""
    for metric,overall_metric_data in metrics_data_map.items():
        if overall_metric_data is None or not bool(overall_metric_data):
            continue

        for metric_data,data_value in overall_metric_data.items():
            if data_value is None or len(data_value) == 0:
                continue
            labels_list = metric_data.split(";")
            metric_str += metric + "_count" + "{" + get_label_format("app_service", service) + ","
            for item in labels_list:
                item_list = item.split('=')
                metric_str += get_label_format(item_list[0], item_list[1]) + ","
            metric_str += "} " + str(data_value[0]) + "\n"

            if data_value[1] is not None:
                metric_str += metric + "_sum" + "{" + "app_service=" + "\"" + service + "\"" + ","
                for item in labels_list:
                    item_list = item.split('=')
                    metric_str += get_label_format(item_list[0], item_list[1]) + ","
                metric_str += "} " + str(data_value[1]) + "\n"
    
    metric_str += buckets_data_string("")
    if not os.path.exists(metrics_file_path):
        os.makedirs(metrics_file_path)
    f = open(metrics_file,"w")
    f.write(metric_str)
    f.close()


def gather_metrics():
    for line in tail_log_file():
        try:
            data = line.split(';')
            metrics_map = {}
            for data_item in data:
                if data_item is None or data_item == "":
                    continue
                pair = data_item.split('=')
                if len(pair) < 2:
                    continue
                metrics_map[pair[0]] = pair[1]

            if not metrics_map:
                continue

            try:
                metric = metrics_map.pop('metric')
            except Exception as e:
                metric = "custom_metric"
    
            try:
                time_taken = float(metrics_map.pop('time_taken'))
            except Exception as e:
                time_taken = None   

            labels_data = []
            for key,value in metrics_map.items():
                if key is None or key == "":
                    continue
                if value is None or value == "":
                    value = "None"
                labels_data.append(key + "=" + value)

            labels_data.sort()
            create_metrics_data(metric, ";".join(labels_data), time_taken)
            create_buckets_data(metric, metrics_map['channel'], metrics_map['uri'], time_taken)
            write_metrics()

        except Exception as e:
            print(line + ". Exception: " + str(e))


def tail_log_file():
    while True:
        try:
            if not os.path.exists(log_file_path):
                os.makedirs(log_file_path)
            if not os.path.exists(log_file):
                open(log_file, 'w').close()
                os.chmod(log_file, 0o777)
            current = open(log_file, "r")
            curino = os.fstat(current.fileno()).st_ino
            current.seek(0, os.SEEK_END)
            while True:
                line = current.readline()
                if not line:
                    sleep(0.1)
                    try:
                        if oct(os.stat(log_file).st_mode)[-3:] != '777':
                            os.chmod(log_file, 0o777)
                        if os.stat(log_file).st_ino != curino:
                            new = open(log_file, "r")
                            current.close()
                            current = new
                            curino = os.fstat(current.fileno()).st_ino
                            continue
                    except IOError:
                        if not os.path.exists(log_file):
                            open(log_file, 'w').close()
                            os.chmod(log_file, 0o777)
                            current = open(log_file, "r")
                            curino = os.fstat(current.fileno()).st_ino
                        pass
                else:
                    line = line.replace('\n','')
                    line = line.replace('\r','')
                    line = line.replace('\b','')
                    line = line.replace('\f','')
                    line = line.replace('\t','')
                    if not line:
                        continue
                    yield line
        except IOError:
            pass

if __name__ == "__main__":
    main()
