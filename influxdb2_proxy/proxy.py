from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

import pandas as pd
import yaml
import os
import atexit


class DatabaseManager:
    START_TIME = '1970-01-01T00:00:00Z'

    def __init__(self, filename: str):
        with open(filename) as file:
            doc = yaml.load(file, Loader=yaml.FullLoader)

            self.token = doc['token']
            self.org = doc['org']
            self.bucket = doc['bucket']
            self.url = doc['url']

        # print(self.url)
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        atexit.register(self.cleanup)

    def add_single_point(self) -> None:
        """
        Add a single record
        """
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        p = Point("spikes").tag("proceed to review", False).tag("resolved", False).field("acc4", 2)

        write_api.write(self.bucket, self.org, record=p)
        write_api.__del__()

    def add_df(self, df: pd.DataFrame, measurement_name: str, sensor_name: str, debug_flag: bool) -> int:
        """
        Upload a panda's dataframe
        :param df: pandas dataframe containing timestamp and measurements values
        :param measurement_name: the name of the measurement
        :param sensor_name: the name of the sensor
        :param debug_flag: print debugging messages
        :return: number of records inserted
        """
        try:
            write_api = self.client.write_api(write_options=SYNCHRONOUS)

            df.columns = ['time', measurement_name]
            df.index = pd.to_datetime(df['time'])
            df = df.drop('time', 1)

            sensor = [sensor_name] * len(df)
            df.insert(1, "sensor", sensor)

            # this is where we label the data. Is it a defect or not?
            review = False * len(df)
            df.insert(2, "proceed to review", review)

            # this is will the help the front-end to avoid displaying resolved issues
            resolved = False * len(df)
            df.insert(3, "resolved", resolved)

            write_api.write(self.bucket, record=df, data_frame_measurement_name=measurement_name,
                            data_frame_tag_columns=['sensor', 'proceed to review', 'resolved'])
            if debug_flag:
                print(f'\nThe script {len(df)} imported in the db')
            write_api.__del__()
            return len(df)
        except Exception as e:
            print(f"I cannot write the given dataframe file to the database.\nMessage:\n{e}")
            return 0

    def add_csv(self, csv_file: str, measurement_name: str, debug_flag: bool) -> int:
        """
        Upload a clean csv file with sensor data (check data folder for samples)
        :param csv_file: the csv file including the entire path
        :param measurement_name: the name of the measurement
        :param debug_flag: print debugging messages
        :return: number of records inserted
        """
        try:
            df = pd.read_csv(csv_file, parse_dates=[0], header=None)
            sensor_name = os.path.splitext(os.path.basename(csv_file))[0]
            records = self.add_df(df, measurement_name, sensor_name, debug_flag)
            return records
        except Exception as e:
            print(f"I cannot write the given csv file to the database.\nMessage:\n{e}")
            return 0

    def query_measurements(self, time_duration: str, debug_flag: bool) -> int:
        """
        Displays the raw data stored in the database the duration requested
        :param time_duration:
        :param debug_flag: print the contents of the bucket
        :return: number of records for the specified measurement
        """
        query_api = self.client.query_api()
        tables = query_api.query('from(bucket:"' + self.bucket + '") |> range(start: -' + time_duration + ')')
        query_api.__del__()

        if not tables:
            if debug_flag:
                print("Nothing to display")
            return 0
        else:
            if debug_flag:
                for table in tables:
                    print(table)
                    for row in table.records:
                        print(row.values)
            return len(tables[0].records)

    def delete_measurements(self, measurement_name) -> int:
        """
        Deletes all the records of the specific measurement
        :param measurement_name: the name of the measurement that we want to delete
        :return: number of records we deleted
        """
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        query = 'from(bucket:"' + self.bucket + '")' \
                                                ' |> range(start: ' + self.START_TIME + ', stop: ' + current_time + ')'
        result = self.client.query_api().query(query=query)

        delete_api = self.client.delete_api()
        delete_api.delete(self.START_TIME, current_time, f"_measurement={measurement_name}",
                          bucket=self.bucket,
                          org=self.org)

        return len(result[0].records)

    def cleanup(self) -> None:
        """
        Close clients after terminate a script.
        :return:
        """
        self.client.__del__()


if __name__ == '__main__':
    x = DatabaseManager('config/credentials.yaml')
    # x.add_single_point()
    # x.add_csv('../data/test.csv', 'spikes', True)

    # records = x.query_measurements('1000d', True)
    # print(records)

    # x.delete_measurements('spikes')
