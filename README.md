# InfluxDb2.0 communication proxy

In this library we experiment with a library that acts as a proxy between the script that identifies an anomaly 
(in our case spikes) and influxdb2.0. For this case, we use the cloud [influxdata cloud](https://cloud2.influxdata.com/signup).

### Connection credentials
There is a yaml file inside the xonfig folder (edit accordingly).

### Importing data
In our case we assume sensors that report the anomaly using a timestamp and the measurement.
For example, the spike detection reports the timestamp and the number of spikes. Sample of the 
csv file that we assume as inputs can be found in the [data](./data) folder.

### Format of the message
We assume a message format that records the defect (anomaly) accompanied with three tags.
- sensor: (string) the name of the sensor (e.g. acc4). The convention is to use it as the name of the csv file
- proceed to review: (boolean) this indicates if the defect will be reviewed or ignored as noise
- resolved: (boolean) this indicates if the issue has been resolved or not

The raw data could be uploaded either in the form of a csv file or as Pandas dataframes.

### Use

Connect to the database:
```
x = DatabaseManager('config/credentials.yaml')
```

Add single point or csv file:
```
x.add_single_point()
x.add_csv('../data/test.csv', 'spikes', True)
```
query measurements:
```
records = x.query_measurements('1000d', True)
print(records)
```
delete measurements:
```
x.delete_measurements('spikes')
```

### Complatibility (versions)
We assume python 3.6.8 that can be enforced using [pyenv](https://github.com/pyenv/pyenv). For the dependency management, we use [poetry](https://python-poetry.org/).
 
