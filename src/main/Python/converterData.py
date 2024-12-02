import urllib.request
import re
import json
import os

url = "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
gz_file = "NASA_access_log_Jul95.gz"
log_file = "NASA_access_log_Jul95"
output_file = "parsed_NASA_access_log.json"

if not os.path.exists(gz_file):
    print("Downloading the log file...")
    urllib.request.urlretrieve(url, gz_file)
    print("Download complete.")

if not os.path.exists(log_file):
    import gzip
    with gzip.open(gz_file, 'rt', encoding='ISO-8859-1') as f_in:
        with open(log_file, 'w', encoding='ISO-8859-1') as f_out:
            f_out.write(f_in.read())
    print("Unzipped the log file.")

def parse_log_line(line):
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d{3}) (\d+|-)'
    match = re.match(pattern, line)

    if match:
        host = match.group(1)
        timestamp = match.group(2)
        request = match.group(3)
        http_reply_code = int(match.group(4))
        bytes_reply = match.group(5)

        if bytes_reply == "-":
            bytes_reply = 0
        else:
            bytes_reply = int(bytes_reply)

        log_data = {
            "host": host,
            "timestamp": timestamp,
            "request": request,
            "http_reply_code": http_reply_code,
            "bytes": bytes_reply
        }
        return log_data
    else:
        return None

parsed_data = []
with open(log_file, 'r', encoding='ISO-8859-1') as file:
    for line in file:
        data = parse_log_line(line)
        if data:
            parsed_data.append(data)

with open(output_file, 'w') as json_file:
    json.dump(parsed_data, json_file, indent=4)

print(f"Data saved in the file {output_file}")
